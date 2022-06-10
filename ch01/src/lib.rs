use std::{
    error::Error,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use redis::{Commands, Connection};

const ONE_WEEK_IN_SECONDS: usize = 7 * 86400;
const VOTE_SCORE: usize = 432;
const ARTICLES_PER_PAGE: isize = 25;

type Article = Vec<(String, String)>;

// Some generic traits that implement Into<String> were thrown in haphazardly because I initially
// had it accept strings only to realize it's nicer for the arguments to accept &str for testing.
// I made some parameters generic because that's ultimately what would be done if we were to actually
// build this out, but only the ones that were easy to do and reduced the time to create the test.
pub fn article_vote<S>(conn: &mut Connection, user: S, article: S) -> Result<(), Box<dyn Error>>
where
    S: Into<String>,
{
    let user = user.into();
    let article = article.into();

    let cutoff = SystemTime::now() - Duration::from_secs(ONE_WEEK_IN_SECONDS as u64);
    let creation_time: u128 = conn.zscore("time:", &article)?;
    if creation_time < cutoff.duration_since(UNIX_EPOCH)?.as_millis() {
        return Err("Cannot upvote posts created more than a week ago.".into());
    }
    let article_id = article
        .split(':')
        .collect::<Vec<_>>()
        .pop()
        .expect("Articles should be namespaced with 'article:'");
    let mut article_votes = "voted:".to_owned();
    article_votes.push_str(article_id);
    if conn.sadd(article_votes, user)? {
        conn.zincr("score:", &article, VOTE_SCORE)?;
        conn.hincr(&article, "votes", 1)?;
    }
    Ok(())
}

pub fn post_article<S>(
    conn: &mut Connection,
    user: S,
    title: S,
    link: S,
) -> Result<String, Box<dyn Error>>
where
    S: Into<String>,
{
    let user = user.into();
    let title = title.into();
    let link = link.into();

    // Attempting `article_id: String` off the bat throws an error.
    let article_id: usize = conn.incr("article:", 1)?;
    let article_id: String = article_id.to_string();

    let mut voted = "voted:".to_owned();
    voted.push_str(&article_id);
    conn.sadd(&voted, &user)?;
    conn.expire(&voted, ONE_WEEK_IN_SECONDS)?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .to_string();
    let mut article = "article:".to_owned();
    article.push_str(&article_id);
    conn.hset_multiple(
        &article,
        &[
            ("title", &title),
            ("link", &link),
            ("poster", &user),
            ("time", &now),
            ("votes", &(1_usize).to_string()),
        ],
    )?;

    conn.zadd(
        "score:",
        &article,
        now.parse::<usize>().unwrap() + VOTE_SCORE,
    )?;
    conn.zadd("time:", &article, &now)?;

    Ok(article_id)
}

pub fn get_articles(
    conn: &mut Connection,
    page: isize,
    order: Option<String>,
) -> Result<Vec<Article>, Box<dyn Error>> {
    let start = (page - 1) * ARTICLES_PER_PAGE;
    let end = start + ARTICLES_PER_PAGE - 1;

    let ids: Vec<String> =
        conn.zrevrange(order.unwrap_or_else(|| "score:".to_owned()), start, end)?;
    let mut articles = vec![];
    for id in ids {
        let mut article_data: Article = conn.hgetall(&id)?;
        article_data.push(("id".to_owned(), id));
        articles.push(article_data);
    }
    Ok(articles)
}

pub fn add_remove_groups<Q, R, S>(
    conn: &mut Connection,
    article_id: Q,
    to_add: Vec<R>,
    to_remove: Vec<S>,
) -> Result<(), Box<dyn Error>>
where
    Q: Into<String>,
    R: Into<String>,
    S: Into<String>,
{
    let mut article = "article:".to_owned();
    article.push_str(&article_id.into());

    for group_label in to_add {
        let mut group = "group:".to_owned();
        group.push_str(&group_label.into());
        conn.sadd(group, &article)?;
    }

    for group_label in to_remove {
        let mut group = "group:".to_owned();
        group.push_str(&group_label.into());
        conn.srem(group, &article)?;
    }
    Ok(())
}

pub fn get_group_articles<S>(
    conn: &mut Connection,
    group: S,
    page: isize,
    order: Option<String>,
) -> Result<Vec<Article>, Box<dyn Error>>
where
    S: Into<String>,
{
    let group = group.into();
    let order = order.unwrap_or_else(|| "score:".to_owned());
    let mut key = order.to_owned();
    key.push_str(&group);
    if !conn.exists(&key)? {
        let mut group_key = "group:".to_owned();
        group_key.push_str(&group);
        conn.zinterstore_max(&key, &[&group_key, &order])?;
        conn.expire(&key, 60)?;
    }
    get_articles(conn, page, Some(key))
}

#[cfg(test)]
mod tests {
    use redis::Commands;

    use crate::{
        add_remove_groups, article_vote, get_articles, get_group_articles, post_article, Article,
    };

    // Execute`cargo test -p ch01 -- --nocapture` to run these tests
    #[test]
    fn test_article_functionality() {
        let mut conn = redis::Client::open("redis://127.0.0.1")
            .expect("Should be able to reach Redis Server")
            .get_connection()
            .expect("Should be able to Establish Connection");

        let article_id =
            post_article(&mut conn, "username", "A title", "http://google.com").unwrap();
        let mut article = "article:".to_owned();
        article.push_str(&article_id);
        println!("We posted a new article with id {article_id} ({article})\n");
        assert!(!article_id.is_empty());

        println!("Its HASH looks like:");
        let r: Article = conn.hgetall(&article).unwrap();
        println!("{:#?}\n", r);
        assert!(r.len() != 0);

        article_vote(&mut conn, "other_user", &article).unwrap();
        let v: usize = conn.hget(&article, "votes").unwrap();
        println!("We voted for the article, it now has votes: {v}\n");
        assert!(v > 1);

        println!("The currently highest-scoring articles are:");
        let articles = get_articles(&mut conn, 1, None).unwrap();
        for article in &articles {
            println!("{article:?}");
        }
        println!();
        assert!(articles.len() >= 1);

        add_remove_groups::<_, _, &str>(&mut conn, article_id, vec!["new-group"], vec![]).unwrap();
        println!("We added the article to a new group, other articles include:");
        let articles = get_group_articles(&mut conn, "new-group", 1, None).unwrap();
        for article in &articles {
            println!("{article:?}");
        }
        println!();
        assert!(articles.len() >= 1);

        let keys = ["article:*", "group:*", "score:*", "time:*", "voted:*"];
        for key in keys {
            let sub_keys: Vec<String> = conn.keys(key).unwrap();
            for sub_key in sub_keys {
                conn.del::<_, usize>(sub_key).unwrap();
            }
        }
    }
}
