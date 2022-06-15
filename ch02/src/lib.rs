use std::{
    cmp,
    collections::hash_map::DefaultHasher,
    error::Error,
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use redis::{Commands, Connection};
use urlparse::urlparse;

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
        conn.zincr("viewed:", item, -1)?;
    }
    Ok(())
}

pub fn clean_sessions(
    conn: &mut Connection,
    limit: isize,
    quit: Arc<AtomicBool>,
) -> Result<(), Box<dyn Error>> {
    while !quit.load(Ordering::Relaxed) {
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
    while !quit.load(Ordering::Relaxed) {
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

pub fn cache_request(
    conn: &mut Connection,
    request: &str,
    callback: &dyn Fn(&str) -> String,
) -> Result<String, Box<dyn Error>> {
    if !can_cache(conn, request)? {
        return Ok(callback(request));
    }

    let mut page_key = "cache:".to_owned();
    page_key.push_str(&hash_request(request));
    let content: String = conn.get(&page_key).unwrap_or_else(|_| callback(request));

    conn.set_ex(&page_key, &content, 300_usize)?;

    Ok(content)
}

pub fn can_cache(conn: &mut Connection, request: &str) -> Result<bool, Box<dyn Error>> {
    let item_id = extract_item_id(request);
    if item_id.is_none() || is_dynamic(request) {
        return Ok(false);
    }
    let rank: Option<usize> = conn.zrank("viewed:", item_id)?;
    Ok(rank.is_some() && rank.unwrap() < 10000)
}

pub fn schedule_row_cache(
    conn: &mut Connection,
    row_id: &str,
    delay: isize,
) -> Result<(), Box<dyn Error>> {
    conn.zadd("delay:", row_id, delay)?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as isize;
    conn.zadd("schedule:", row_id, now)?;
    Ok(())
}

pub fn cache_rows(conn: &mut Connection, quit: Arc<AtomicBool>) -> Result<(), Box<dyn Error>> {
    while !quit.load(Ordering::Relaxed) {
        let next: Vec<(String, isize)> = conn.zrange_withscores("schedule:", 0, 0)?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as isize;
        if next.is_empty() || next[0].1 > now {
            thread::sleep(Duration::from_millis(50));
            continue;
        }

        let row_id = next[0].0.to_string();
        let delay: isize = conn.zscore("delay:", &row_id)?;
        let mut inv = "inv:".to_owned();
        inv.push_str(&row_id);

        if delay <= 0 {
            conn.zrem("delay:", &row_id)?;
            conn.zrem("schedule:", &row_id)?;
            conn.del(&inv)?;
            continue;
        }

        // In a real scenario there might be more work to get it into a processable format,
        // but for now an Inventory module is used to mock a real call to a database.
        let row = Inventory::get(&row_id);
        conn.zadd("schedule:", &row_id, now + delay)?;
        conn.set(&inv, serde_json::to_string(&row)?)?;
    }
    Ok(())
}

// ---------------------- Below this line are helpers to test the code ----------------------
fn extract_item_id(request: &str) -> Option<String> {
    let parsed = urlparse(request);
    if let Some(query) = parsed.get_parsed_query() {
        if let Some(value) = query.get("item") {
            return Some(value[0].clone());
        }
    }
    None
}

fn is_dynamic(request: &str) -> bool {
    let parsed = urlparse(request);
    if let Some(query) = parsed.get_parsed_query() {
        return query.contains_key("_");
    }
    false
}

fn hash_request(request: &str) -> String {
    let mut hasher = DefaultHasher::new();
    request.hash(&mut hasher);
    hasher.finish().to_string()
}

#[allow(non_snake_case)]
mod Inventory {
    use std::{
        collections::BTreeMap,
        time::{SystemTime, UNIX_EPOCH},
    };

    // Inventory::get() represents a call to the database for more information on a product with id row_id
    // BTreeMap just so that printing it out gives consistent ordering to the fields
    pub(crate) fn get(row_id: &str) -> BTreeMap<&str, String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();

        BTreeMap::from([
            ("id", row_id.to_owned()),
            ("data", String::from("data to cache...")),
            ("cached", now),
        ])
    }
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

    use crate::{
        add_to_cart, cache_request, cache_rows, can_cache, check_token, clean_full_sessions,
        clean_sessions, schedule_row_cache, update_token,
    };
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

    #[test]
    fn test_cache_request() {
        let mut conn = redis::Client::open("redis://127.0.0.1")
            .expect("Should be able to reach Redis Server")
            .get_connection()
            .expect("Should be able to Establish Connection");

        let token = Uuid::new_v4().to_string();

        fn callback(request: &str) -> String {
            let mut content = "content for ".to_owned();
            content.push_str(request);
            content
        }

        update_token(&mut conn, &token, "username", Some("itemX")).expect("Token should update");
        let url = "http://test.com/?item=itemX";
        println!("We are going to cache a simple request against {url}");
        let result =
            cache_request(&mut conn, url, &callback).expect("Caching the request shouldn't err");
        println!("We got initial content: {result}\n");

        assert!(!result.is_empty());

        println!("To test that we've cached the request, we'll pass a bad callback");
        let result2 = cache_request(&mut conn, url, &|_request: &str| -> String {
            String::new()
        })
        .expect("Caching the request shouldn't err");
        println!("We ended up getting the same response! {result2}");

        assert_eq!(result, result2);

        assert!(!can_cache(&mut conn, "http://test.com")
            .expect("Checking for ability to cache shouldn't err"));
        assert!(
            !can_cache(&mut conn, "http://test.com/?item=itemX&_=1234536")
                .expect("Checking for ability to cache shouldn't err")
        );
    }

    #[test]
    fn test_cache_rows() {
        let mut conn = redis::Client::open("redis://127.0.0.1")
            .expect("Should be able to reach Redis Server")
            .get_connection()
            .expect("Should be able to Establish Connection");

        let quit = Arc::new(AtomicBool::new(false));

        println!("First, let's schedule caching of itemX every 5 seconds");
        schedule_row_cache(&mut conn, "itemX", 5)
            .expect("itemX should be scheduled to cache every 5 seconds");
        let s: Vec<(String, String)> = conn.zrange_withscores("schedule:", 0, -1).unwrap();
        println!("Our schedule looks like: {s:?}");

        println!("We'll start a caching thread that will cache the data...");
        let signal = Arc::clone(&quit);
        thread::spawn(move || cache_rows(&mut conn, signal).unwrap());
        thread::sleep(Duration::from_millis(5)); // wait for cache_rows thread to cache

        let mut conn = redis::Client::open("redis://127.0.0.1")
            .expect("Should be able to reach Redis Server")
            .get_connection()
            .expect("Should be able to Establish Connection");
        let r: String = conn.get("inv:itemX").unwrap();
        println!("Our cached data looks like:\n{r}\n");
        assert!(!r.is_empty());
        println!("We'll check again in 5 seconds...");
        thread::sleep(Duration::from_secs(5));
        println!("Notice that the data has changed...");
        let r2: String = conn.get("inv:itemX").unwrap();
        println!("{r2}\n");
        assert_ne!(r, r2);

        println!("Let's force uncaching");
        schedule_row_cache(&mut conn, "itemX", -1).unwrap();
        thread::sleep(Duration::from_secs(1));
        let r: Option<String> = conn.get("inv:itemX").unwrap();
        println!(
            "Was the cache cleared? {}\n",
            if r.is_some() { "no" } else { "yes" }
        );
        assert!(r.is_none());

        quit.store(true, Ordering::Relaxed);
        thread::sleep(Duration::from_secs(1));

        // Currently checking to see if the thread is still running by asserting that the only reference to the boolean
        // is the test_login_cookies function; when it's stabilized, this could be replaced with t.is_running()
        if Arc::strong_count(&quit) != 1 {
            panic!("The database caching thread is still allive?!?");
        }
    }
}
