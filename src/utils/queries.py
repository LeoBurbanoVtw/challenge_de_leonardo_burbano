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


q2 = '''
    WITH A AS (
    select REGEXP_EXTRACT_ALL(content, r"(?:[\U0001F300-\U0001F5FF]|[\U0001F900-\U0001F9FF]|[\U0001F600-\U0001F64F]|[\U0001F680-\U0001F6FF]|[\U00002600-\U000026FF]\uFE0F?|[\U00002700-\U000027BF]\uFE0F?|\u24C2\uFE0F?|[\U0001F1E6-\U0001F1FF]{1,2}|[\U0001F170\U0001F171\U0001F17E\U0001F17F\U0001F18E\U0001F191-\U0001F19A]\uFE0F?|[\u0023\u002A\u0030-\u0039]\uFE0F?\u20E3|[\u2194-\u2199\u21A9-\u21AA]\uFE0F?|[\u2B05-\u2B07\u2B1B\u2B1C\u2B50\u2B55]\uFE0F?|[\u2934\u2935]\uFE0F?|[\u3297\u3299]\uFE0F?|[\U0001F201-\U0001F202\U0001F21A\U0001F22F\U0001F232\U0001F23A\U0001F250\U0001F251]\uFE0F?|[\u203C-\u2049]\uFE0F?|[\u00A9-\u00AE]\uFE0F?|[\u2122\u2139]\uFE0F?|\U0001F004\uFE0F?|\U0001F0CF\uFE0F?|[\u231A\u231B\u2328\u23CF\u23E9\u23F3\u23F8\u23FA]\uFE0F?)") AS emoji
    from `de-leonardo-burbano.DE_BIGQUERY_LB.test0006`
    WHERE ARRAY_LENGTH(REGEXP_EXTRACT_ALL(content, r"(?:[\U0001F300-\U0001F5FF]|[\U0001F900-\U0001F9FF]|[\U0001F600-\U0001F64F]|[\U0001F680-\U0001F6FF]|[\U00002600-\U000026FF]\uFE0F?|[\U00002700-\U000027BF]\uFE0F?|\u24C2\uFE0F?|[\U0001F1E6-\U0001F1FF]{1,2}|[\U0001F170\U0001F171\U0001F17E\U0001F17F\U0001F18E\U0001F191-\U0001F19A]\uFE0F?|[\u0023\u002A\u0030-\u0039]\uFE0F?\u20E3|[\u2194-\u2199\u21A9-\u21AA]\uFE0F?|[\u2B05-\u2B07\u2B1B\u2B1C\u2B50\u2B55]\uFE0F?|[\u2934\u2935]\uFE0F?|[\u3297\u3299]\uFE0F?|[\U0001F201-\U0001F202\U0001F21A\U0001F22F\U0001F232\U0001F23A\U0001F250\U0001F251]\uFE0F?|[\u203C-\u2049]\uFE0F?|[\u00A9-\u00AE]\uFE0F?|[\u2122\u2139]\uFE0F?|\U0001F004\uFE0F?|\U0001F0CF\uFE0F?|[\u231A\u231B\u2328\u23CF\u23E9\u23F3\u23F8\u23FA]\uFE0F?)"))>0
    )
    select element, count(1) as frecuency
    FROM A, UNNEST(emoji) AS element
    GROUP BY element
    order by 2 desc
    limit 10
'''


q3 = """
    
"""