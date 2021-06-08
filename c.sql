with committers (id, committer_date) as (
    select *
    from (values
        ('amansinha',DATE '2016-01-01'),
        ('sereda',DATE '2016-01-01'),
        ('hashutosh',DATE '2016-01-01'),
        ('cbaynes',DATE '2016-01-01'),
        ('cbeikov',DATE '2016-01-01'),
        ('chunwei',DATE '2016-01-01'),
        ('danny0405',DATE '2016-01-01'),
        ('ebegoli',DATE '2016-01-01'),
        ('fengzhu',DATE '2016-01-01'),
        ('forwardzhu',DATE '2016-01-01'),
        ('francischuang',DATE '2016-01-01'),
        ('gian',DATE '2016-01-01'),
        ('hyuan',DATE '2016-01-01'),
        ('hongze',DATE '2016-01-01'),
        ('jamestaylor',DATE '2016-01-01'),
        ('jacques',DATE '2016-01-01'),
        ('jbalint',DATE '2016-01-01'),
        ('jcamacho',DATE '2016-01-01'),
        ('jni',DATE '2016-01-01'),
        ('jinxing',DATE '2016-01-01'),
        ('jpullokk',DATE '2016-01-01'),
        ('elserj',DATE '2016-01-01'),
        ('jfeinauer',DATE '2016-01-01'),
        ('jhyde',DATE '2016-01-01'),
        ('kliew',DATE '2016-01-01'),
        ('krisden',DATE '2016-01-01'),
        ('laurent',DATE '2016-01-01'),
        ('liyafan',DATE '2016-01-01'),
        ('maryannxue',DATE '2016-01-01'),
        ('mmior',DATE '2016-01-01'),
        ('milinda',DATE '2016-01-01'),
        ('minji',DATE '2016-01-01'),
        ('mgelbana',DATE '2016-01-01'),
        ('ndimiduk',DATE '2016-01-01'),
        ('nishant',DATE '2016-01-01'),
        ('rubenql',DATE '2016-01-01'),
        ('amaliujia',DATE '2016-01-01'),
        ('snuyanzin',DATE '2016-01-01'),
        ('shuyichen',DATE '2016-01-01'),
        ('bslim',DATE '2016-01-01'),
        ('zabetak',DATE '2016-01-01'),
        ('stevenn',DATE '2016-01-01'),
        ('tdunning',DATE '2016-01-01'),
        ('vgarg',DATE '2016-01-01'),
        ('vladimirsitnikov',DATE '2016-01-01'),
        ('volodymyr',DATE '2016-01-01'),
        ('yanlin',DATE '2016-01-01'),
        ('zhenw',DATE '2016-01-01'),
        ('zhiqianghe',DATE '2016-01-01'),
        ('zhiwei',DATE '2016-01-01'),
        ('kgyrtkirk',DATE '2016-01-01'))),
  committer_emails (id, email) as (
    select id, id || '@apache.org' as email
    from committers)
select extract(year from ym) as `Year`,
  extract(month from ym) as `Month`,
  c,
  d
from (
  select floor(c.commit_timestamp to month) as ym, count(*) as c, min(x.committer_date) AS d
  from os.git_commits AS c
  left join committer_emails AS e on REGEXP_REPLACE(REGEXP_REPLACE(c.author, '.*<', ''), '>', '') = e.email
  left join committers AS x on e.id = x.id
  group by floor(c.commit_timestamp to month)
  order by floor(c.commit_timestamp to month) desc limit 24);

with committers (id, committer_date) as (
    select cast(id as varchar(20)) as id, committer_date
    from (values
        ('amansinha',DATE '2016-01-01'),
        ('sereda',DATE '2016-01-01'),
        ('hashutosh',DATE '2016-01-01'),
        ('cbaynes',DATE '2016-01-01'),
        ('cbeikov',DATE '2016-01-01'),
        ('chunwei',DATE '2016-01-01'),
        ('danny0405',DATE '2016-01-01'),
        ('ebegoli',DATE '2016-01-01'),
        ('fengzhu',DATE '2016-01-01'),
        ('forwardzhu',DATE '2016-01-01'),
        ('francischuang',DATE '2016-01-01'),
        ('gian',DATE '2016-01-01'),
        ('hyuan',DATE '2016-01-01'),
        ('hongze',DATE '2016-01-01'),
        ('jamestaylor',DATE '2016-01-01'),
        ('jacques',DATE '2016-01-01'),
        ('jbalint',DATE '2016-01-01'),
        ('jcamacho',DATE '2016-01-01'),
        ('jni',DATE '2016-01-01'),
        ('jinxing',DATE '2016-01-01'),
        ('jpullokk',DATE '2016-01-01'),
        ('elserj',DATE '2016-01-01'),
        ('jfeinauer',DATE '2016-01-01'),
        ('jhyde',DATE '2016-01-01'),
        ('kliew',DATE '2016-01-01'),
        ('krisden',DATE '2016-01-01'),
        ('laurent',DATE '2016-01-01'),
        ('liyafan',DATE '2016-01-01'),
        ('maryannxue',DATE '2016-01-01'),
        ('mmior',DATE '2016-01-01'),
        ('milinda',DATE '2016-01-01'),
        ('minji',DATE '2016-01-01'),
        ('mgelbana',DATE '2016-01-01'),
        ('ndimiduk',DATE '2016-01-01'),
        ('nishant',DATE '2016-01-01'),
        ('rubenql',DATE '2016-01-01'),
        ('amaliujia',DATE '2016-01-01'),
        ('snuyanzin',DATE '2016-01-01'),
        ('shuyichen',DATE '2016-01-01'),
        ('bslim',DATE '2016-01-01'),
        ('zabetak',DATE '2016-01-01'),
        ('stevenn',DATE '2016-01-01'),
        ('tdunning',DATE '2016-01-01'),
        ('vgarg',DATE '2016-01-01'),
        ('vladimirsitnikov',DATE '2016-01-01'),
        ('volodymyr',DATE '2016-01-01'),
        ('yanlin',DATE '2016-01-01'),
        ('zhenw',DATE '2016-01-01'),
        ('zhiqianghe',DATE '2016-01-01'),
        ('zhiwei',DATE '2016-01-01'),
        ('kgyrtkirk',DATE '2016-01-01')) as t (id, committer_date)),
  committer_emails (id, email) as (
    select id, id || '@apache.org' as email
    from committers
    union
    select cast(id as varchar(20)), cast(email as varchar(50)) from (values
      ('jhyde','julianhyde@gmail.com'),
      ('jhyde','jhyde@apache.org'),
      ('jhyde','julian@hydromatic.net')) as t (id, email)),
  c2 as (select *, cast(REGEXP_REPLACE(REGEXP_REPLACE(author, '.*<', ''), '>.*', '') as varchar(50)) as email
    from os.git_commits),
  clog as (
    select c.author, e.email, e.id, c.email as e2
    from c2 AS c
    left join committer_emails AS e on c.email = e.email)
select author, '['||e2||']', '['||email||']', count(*)
from clog
group by author, e2, email
order by count(*) desc limit 20;

# End
