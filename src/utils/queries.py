q1 = """
    with highdates as (
        SELECT date(`date`) `date`, count(1)
        FROM `de-leonardo-burbano.DE_BIGQUERY_LB.test0006`
        group by date(`date`)
        order by 2 desc
        limit 10)
    , users as (
        select b.username, date(b.`date`) as date_agg, COUNT(1) as num_tweets
        from `de-leonardo-burbano.DE_BIGQUERY_LB.test0006` as b
        inner join highdates on highdates.`date` = date(B.`date`)
        group by b.username, date(b.`date`)
    )
    , c as (
        select username, date_agg, row_number() over(partition by date_agg order by num_tweets desc) as num
        from users
    )
    select *
    from c
    where num = 1
"""