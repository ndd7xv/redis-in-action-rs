use std::{
    cmp,
    error::Error,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use redis::{Commands, Connection};

pub fn check_token(conn: &mut Connection, token: &str) -> Result<String, Box<dyn Error>> {
    Ok(conn.hget("login:", token)?)
}

pub fn update_token(
    conn: &mut Connection,
    token: &str,
    user: &str,
    item: Option<&str>,
) -> Result<(), Box<dyn Error>> {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as usize;
    conn.hset("login:", &token, user)?;
    conn.zadd("recent:", &token, timestamp)?;

    if let Some(item) = item {
        let mut viewed = String::from("viewed:");
        viewed.push_str(token);

        conn.zadd(&viewed, item, timestamp)?;
        conn.zremrangebyrank(&viewed, 0, -26)?;
    }
    Ok(())
}

pub fn clean_sessions(
    conn: &mut Connection,
    limit: isize,
    quit: Arc<AtomicBool>,
) -> Result<(), Box<dyn Error>> {
    loop {
        if quit.load(Ordering::Relaxed) {
            break;
        }
        let size: isize = conn.zcard("recent:")?;
        if size <= limit {
            thread::sleep(Duration::from_secs(1));
            continue;
        }

        let end_index = cmp::min(size - limit, 100);
        let tokens: Vec<String> = conn.zrange("recent:", 0, end_index - 1)?;
        let views = tokens
            .iter()
            .map(|x| {
                let mut view = "viewed:".to_owned();
                view.push_str(x);
                view
            })
            .collect::<Vec<String>>();

        conn.del(&views)?;
        conn.hdel("login:", &tokens)?;
        conn.zrem("recent:", &tokens)?;
    }
    Ok(())
}

pub fn add_to_cart(
    conn: &mut Connection,
    session: &str,
    item: &str,
    count: isize,
) -> Result<(), Box<dyn Error>> {
    let mut key = "cart:".to_owned();
    key.push_str(session);

    if count <= 0 {
        conn.hdel(key, item)?;
    } else {
        conn.hset(key, item, count)?;
    }
    Ok(())
}

pub fn clean_full_sessions(
    conn: &mut Connection,
    limit: isize,
    quit: Arc<AtomicBool>,
) -> Result<(), Box<dyn Error>> {
    loop {
        if quit.load(Ordering::Relaxed) {
            break;
        }
        let size: isize = conn.zcard("recent:")?;
        if size <= limit {
            thread::sleep(Duration::from_secs(1));
            continue;
        }

        let end_index = cmp::min(size - limit, 100);
        let sessions: Vec<String> = conn.zrange("recent:", 0, end_index - 1)?;
        let session_keys = sessions
            .iter()
            .flat_map(|x| {
                let mut view = "viewed:".to_owned();
                view.push_str(x);

                let mut cart = "cart:".to_owned();
                cart.push_str(x);

                [view, cart]
            })
            .collect::<Vec<String>>();

        conn.del(&session_keys)?;
        conn.hdel("login:", &sessions)?;
        conn.zrem("recent:", &sessions)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread,
        time::Duration,
    };

    use redis::Commands;
    use uuid::Uuid;

    use crate::{add_to_cart, check_token, clean_full_sessions, clean_sessions, update_token};

    // Execute`cargo test -p ch02 -- --nocapture --test-threads 1` to run these tests
    // specifying 1 test thread means one test runs at a time so things run sequentially
    #[test]
    fn test_login_cookies() {
        let mut conn = redis::Client::open("redis://127.0.0.1")
            .expect("Should be able to reach Redis Server")
            .get_connection()
            .expect("Should be able to Establish Connection");

        let token = Uuid::new_v4().to_string();

        update_token(&mut conn, &token, "username", Some("itemX")).expect("Token should update");
        println!("We just logged-in/updated token: {token}");
        println!("For user: 'username'\n");

        println!("What username do we get when we look-up that token?");
        let username = check_token(&mut conn, &token).expect("Token lookup should return username");
        println!("{username}\n");
        assert!(username.eq("username"));

        println!("Let's drop the maximum number of cookies to 0 to clean them out");
        println!("We will start a thread to do the cleaning, while we stop it later");

        let limit = 0;
        let quit = Arc::new(AtomicBool::new(false));

        let signal = Arc::clone(&quit);
        let _t = thread::spawn(move || clean_sessions(&mut conn, limit, signal).unwrap());
        thread::sleep(Duration::from_secs(1));
        assert!(Arc::strong_count(&quit) == 2);
        quit.store(true, Ordering::Relaxed);
        thread::sleep(Duration::from_secs(1));

        // Currently checking to see if the thread is still running by asserting that the only reference to the boolean
        // is the test_login_cookies function; when it's stabilized, this could be replaced with t.is_running()
        if Arc::strong_count(&quit) != 1 {
            panic!("The clean sessions thread is still allive?!?");
        }

        let mut conn = redis::Client::open("redis://127.0.0.1")
            .expect("Should be able to reach Redis Server")
            .get_connection()
            .expect("Should be able to Establish Connection");

        let s: usize = conn.hlen("login:").unwrap();
        println!("The current number of sessions still available is: {s}");
    }

    #[test]
    fn test_shopping_cart_cookies() {
        let mut conn = redis::Client::open("redis://127.0.0.1")
            .expect("Should be able to reach Redis Server")
            .get_connection()
            .expect("Should be able to Establish Connection");

        let limit = 0;
        let quit = Arc::new(AtomicBool::new(false));
        let token = Uuid::new_v4().to_string();

        println!("We'll refresh our session...");
        update_token(&mut conn, &token, "username", Some("itemX")).expect("Token should update");
        println!("And add an item to the shopping cart");
        add_to_cart(&mut conn, &token, "itemY", 3).expect("itemY should be added to shopping cart");
        let mut cart = "cart:".to_owned();
        cart.push_str(&token);
        let r: Vec<(String, String)> = conn.hgetall(&cart).unwrap();
        println!("Our shopping cart currently has: {r:?}\n");

        assert!(r.len() >= 1);

        println!("Let's clean out our sessions and carts");
        let signal = Arc::clone(&quit);
        let _t = thread::spawn(move || clean_full_sessions(&mut conn, limit, signal).unwrap());
        thread::sleep(Duration::from_secs(1));
        assert!(Arc::strong_count(&quit) == 2);
        quit.store(true, Ordering::Relaxed);
        thread::sleep(Duration::from_secs(1));

        // Currently checking to see if the thread is still running by asserting that the only reference to the boolean
        // is the test_login_cookies function; when it's stabilized, this could be replaced with t.is_running()
        if Arc::strong_count(&quit) != 1 {
            panic!("The clean sessions thread is still allive?!?");
        }

        let mut conn = redis::Client::open("redis://127.0.0.1")
            .expect("Should be able to reach Redis Server")
            .get_connection()
            .expect("Should be able to Establish Connection");

        let r: Vec<(String, String)> = conn.hgetall(&cart).unwrap();
        println!("Our shopping cart now contains: {r:?}");
        assert!(r.len() == 0);
    }
}
