q1 = """
    with highdates as (
        SELECT date(`date`) `date`, count(1) as count_by_date
        FROM `de-leonardo-burbano.DE_BIGQUERY_LB.test0006`
        group by date(`date`)
        order by 2 desc
        limit 10)
    , users as (
        select b.username, date(b.`date`) as date_agg, highdates.count_by_date, COUNT(1) as num_tweets
        from `de-leonardo-burbano.DE_BIGQUERY_LB.test0006` as b
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