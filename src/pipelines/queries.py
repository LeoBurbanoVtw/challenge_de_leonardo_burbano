q1 = """
    with highdates as (
        SELECT date(`date`) `date`, count(1) as count_by_date
        FROM `{dataset_id}.{table_id}`
        group by date(`date`)
        order by 2 desc
        limit 10)
    , users as (
        select b.username, date(b.`date`) as date_agg, highdates.count_by_date, COUNT(1) as num_tweets
        from `{dataset_id}.{table_id}` as b
        inner join highdates on highdates.`date` = date(B.`date`)
        group by b.username, date(b.`date`), highdates.count_by_date
    )
    , c as (
        select username, date_agg, count_by_date, row_number() over(partition by date_agg order by num_tweets desc) as num
        from users
    )
    select date_agg, username
    from c
    where num = 1
    order by count_by_date desc
"""


q2 = '''
    WITH A AS (
    SELECT REGEXP_EXTRACT_ALL(content, {regex_pattern}) AS emoji
    FROM `{dataset_id}.{table_id}`
    WHERE ARRAY_LENGTH(REGEXP_EXTRACT_ALL(content, {regex_pattern})) > 0
    )
    SELECT element, COUNT(1) AS frequency
    FROM A, UNNEST(emoji) AS element
    GROUP BY element
    ORDER BY 2 DESC
    LIMIT 10
'''


q3 = """
    with a as (
        SELECT mentioned.username mentioned_username, mentioned.userid mentioned_userid, A.id, A.username
        FROM  `{dataset_id}.{table_id}` AS A, UNNEST(A.mentionedUsers) as mentioned
        WHERE mentioned.username is not null
    )
    select mentioned_username, count(1)
    from a
    where a.mentioned_username != username
    group by 1
    order by 2 desc
    limit 10
"""