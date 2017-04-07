#!/bin/bash
# use bash to wrap vsql as an interpreter
output=$(sed '1,/^exit/d' $0 | /opt/vertica/bin/vsql -h vertica-lb01 -w`cat /etc/vertica-password` 2>&1)
echo "$output"
return=$(echo "$output" | grep -c 'ERROR')
exit $return

-- user_ids for all games for past 35 days

CREATE local temp TABLE temp_dau ON COMMIT PRESERVE ROWS AS
(
select distinct app, event_day, synthetic_id::varchar(80) as user_id
from gsnmobile.events_dau
where app in ('GSN Casino', 'TriPeaks Solitaire', 'GSN Grand Casino', 'WoF App')
and event_day between sysdate-35 and sysdate-1
    
UNION

select distinct 'GSN.com' as app, event_day, attr4::varchar(80)
from visitors.events
where attr1='gsn.com'
and event_name='pageview'
and event_day between sysdate-35 and sysdate-1

UNION

select distinct 'GSN Canvas' as app, event_day, fb_user_id::varchar(80)
from newapi.events_dau_activity
where platform='canvas'
and activity='casino'
and event_day between sysdate-35 and sysdate-1
  
UNION

select distinct 'GSN Cash Games', event_time::date, attr4::varchar(80)
from visitors.events
where event_time::date between sysdate-35 and sysdate-1
and event_name='pageview'
and attr1='worldwinner.com'

UNION
        
select distinct 'Slots Bash', event_on::date, user_id::varchar(80)
from bash.slots_events
where event_on::date between sysdate-35 and sysdate-1

UNION

select distinct case when device_platform = 'pc' then 'Bingo Bash Canvas' else 'Bingo Bash Mobile' end, active_on::date, user_id::varchar(80)
from bash.bingo_dau
where device_platform <> 'gsn_pc'
and active_on::date between sysdate-35 and sysdate-1

UNION

select distinct 'Fresh Deck Poker', login_time::date, uid::varchar(80)
from poker.player_access_log
where login_time::date between sysdate-35 and sysdate-1

UNION

select distinct 'Sparcade', event_day, device_id::varchar(80)
from arena.events_mobile_dad_all
where event_day between sysdate-35 and sysdate-1
)
; 
-- delete last 3 days from kpi.dim_app_daily_metrics
delete from kpi.dim_app_daily_metrics where event_day >= sysdate::date-3


--GSN.com Casino
insert into kpi.dim_app_daily_metrics(event_day,app_name,transactional_bookings,daily_active_users,monthly_active_users,installs,advertising_revenue)   
select
  d.event_day,
  'GSN.com' as app,
  ifnull(t.transactional_bookings,0) as transactional_bookings,
  ifnull(u.daily_active_users,0) as daily_active_users,
  ifnull(m.monthly_active_users,0) as monthly_active_users,
  ifnull(i.installs,0) as installs,
  ifnull(a.ad_rev,0) as ad_revenue
from (select thedate as event_day from newapi.dim_date where thedate between sysdate::date-3 and sysdate-1) d
left join (
	select 
	  event_day,
	  sum(amount_paid) as transactional_bookings
	from gsncom.events_direct_payment_received
	where event_day between sysdate::date-3 and sysdate-1
	and ifnull(device,'web')='web'
	group by 1
) t using(event_day)
left join (
	select
	  event_day,
	  count(distinct user_id) as daily_active_users
	from temp_dau
	where app = 'GSN.com'
	and event_day between sysdate::date-3 and sysdate-1
	group by 1
	order by 1 desc
) u using(event_day)
left join (
	select
	  d2.event_day,
	  count(distinct user_id) as monthly_active_users
	from (select thedate as event_day from newapi.dim_date where thedate between sysdate::date-3 and sysdate-1) d2
	join temp_dau a on a.event_day between d2.event_day - 29 and d2.event_day and a.app = 'GSN.com'
	where d2.event_day between sysdate::date-3 and sysdate-1
	group by 1
) m using(event_day)
left join (
    select
      event_day,
      count(distinct user_id) as installs
    from (
        select
          user_id,
          min(event_day) as event_day
        from gsncom.events_dau
        where platform='web'
        group by 1
    ) i2
    group by 1
    --order by 1
) i using(event_day)
left join (
	select
	  event_date::date as event_day,
	  sum(revenue) as ad_rev
	from ads.daily_aggregation
	where event_date::date between sysdate::date-3 and sysdate-1
	and vendor <> 'UnityAds'
        and app = 'gsn.com'
	group by 1
	--order by 1 desc
) a using(event_day)
order by 1 desc
;

--Facebook Casino
insert into kpi.dim_app_daily_metrics(event_day,app_name,transactional_bookings,daily_active_users,montly_active_users,installs,advertising_revenue)
select 
  d.event_day,
  'GSN Canvas' as app,
  ifnull(cr.transactional_bookings,0) as transactional_bookings,
  ifnull(u.daily_active_users,0) as daily_active_users,
  ifnull(m.monthly_active_users,0) as monthly_active_users,
  ifnull(i.installs,0) as installs,
  ifnull(a.ad_rev,0) as ad_revenue
from (select thedate as event_day from newapi.dim_date where thedate between sysdate::date-3 and sysdate-1) d
left join (
	select
	  event_day,
	  sum(amount_paid) as transactional_bookings
	from newapi.events_direct_payment_received r
	where event_day between sysdate::date-3 and sysdate-1
	and ifnull(device,'web')='web'
	and payment_type_id in (8,13)
	group by 1
) cr using(event_day)
left join (
	select
	  event_day,
	  count(distinct user_id) as daily_active_users
	from temp_dau
	where app = 'GSN Canvas'
	and event_day between sysdate::date-3 and sysdate-1
	group by 1
	order by 1 desc
) u using(event_day)
left join (
	select
	  d2.event_day,
	  count(distinct user_id) as monthly_active_users
	from (select thedate as event_day from newapi.dim_date where thedate between sysdate::date-3 and sysdate-1) d2
	join temp_dau a on a.event_day between d2.event_day - 29 and d2.event_day and a.app = 'GSN Canvas'
	where d2.event_day between sysdate::date-3 and sysdate-1
	group by 1
) m using(event_day)
left join (
    select
      event_day,
      count(distinct fb_user_id) as installs
    from newapi.dim_first_install
    where device='web'
    group by 1
) i using(event_day)
left join (
	select
	  event_date::date as event_day,
	  sum(revenue) as ad_rev
	from ads.daily_aggregation
	where event_date::date between sysdate::date-3 and sysdate-1
	and vendor <> 'UnityAds'
        and app = 'gsn casino'
        and platform = 'facebook'
	group by 1
	--order by 1 desc
) a using(event_day)
order by 1 desc
;

--Mobile Casino
insert into kpi.dim_app_daily_metrics(event_day,app_name,transactional_bookings,daily_active_users,montly_active_users,installs,advertising_revenue)
select 
  d.event_day,
  'GSN Casino' as app_name,
  ifnull(p.bookings,0) as transactional_bookings,
  ifnull(u.daily_active_users,0) as daily_active_users,
  ifnull(m.monthly_active_users,0) as monthly_active_users,
  ifnull(i.installs,0) as installs,
  ifnull(a.ad_rev,0) as ad_revenue
from (select thedate as event_day from newapi.dim_date where thedate between sysdate::date-3 and sysdate-1) d
left join (
    select
      event_day,
      sum(amount_paid_usd) as bookings
    from gsnmobile.events_payments
    where app='GSN Casino'
    group by 1
) p using(event_day)
left join (
	select
	  event_day,
	  count(distinct user_id) as daily_active_users
	from temp_dau
	where app = 'GSN Casino'
	and event_day between sysdate::date-3 and sysdate-1
	group by 1
	order by 1 desc
) u using(event_day)
left join (
	select
	  d2.event_day,
	  count(distinct user_id) as monthly_active_users
	from (select thedate as event_day from newapi.dim_date where thedate between sysdate::date-3 and sysdate-1) d2
	join temp_dau a on a.event_day between d2.event_day - 29 and d2.event_day and a.app = 'GSN Casino'
	where d2.event_day between sysdate::date-3 and sysdate-1
	group by 1
) m using(event_day)
left join (    select
      first_seen as event_day,
      count(distinct synthetic_id) as installs
    from gsnmobile.dim_app_installs
    where app='GSN Casino'
    group by 1
) i using(event_day)
left join (
	select
	  event_date::date as event_day,
	  sum(revenue) as ad_rev
	from ads.daily_aggregation
	where event_date::date between sysdate::date-3 and sysdate-1
	and vendor <> 'UnityAds'
        and app = 'gsn casino'
        and platform <> 'facebook'
	group by 1
	--order by 1 desc
) a using(event_day)
order by 1 desc
;

-- TriPeaks
insert into kpi.dim_app_daily_metrics(event_day,app_name,transactional_bookings,daily_active_users,montly_active_users,installs,advertising_revenue)
select 
  a.event_day,
  'TriPeaks Solitaire' as app_name,
  ifnull(p.bookings,0) as transactional_bookings,
  ifnull(u.daily_active_users,0) as daily_active_users,
  ifnull(m.monthly_active_users,0) as monthly_active_users,
  ifnull(i.installs,0) as installs,
  ifnull(a2.ad_rev,0) as ad_revenue
from (select thedate as event_day from newapi.dim_date where thedate between sysdate::date-3 and sysdate-1) a 
left join (
	select
	  event_day,
	  count(distinct user_id) as daily_active_users
	from temp_dau
	where app = 'TriPeaks Solitaire'
	and event_day between sysdate::date-3 and sysdate-1
	group by 1
	order by 1 desc
) u using(event_day)
left join (
	select
	  d2.event_day,
	  count(distinct user_id) as monthly_active_users
	from (select thedate as event_day from newapi.dim_date where thedate between sysdate::date-3 and sysdate-1) d2
	join temp_dau a on a.event_day between d2.event_day - 29 and d2.event_day and a.app = 'TriPeaks Solitaire'
	where d2.event_day between sysdate::date-3 and sysdate-1
	group by 1
) m using(event_day)
left join (
    select 
      event_day,
      sum(amount_paid_usd) as bookings
    from gsnmobile.events_payments
    where app='TriPeaks Solitaire'
    group by 1
) p using(event_day)
left join (
    select
      first_seen as event_day,
      count(distinct synthetic_id) as installs
    from gsnmobile.dim_app_installs
    where app='TriPeaks Solitaire'
    group by 1
) i using(event_day)
left join (
	select
	  event_date::date as event_day,
	  sum(revenue) as ad_rev
	from ads.daily_aggregation
	where event_date::date between sysdate::date-3 and sysdate-1
	and vendor <> 'UnityAds'
        and app = 'tripeaks solitaire'
	group by 1
	--order by 1 desc
) a2 using(event_day)
order by 1 desc
;

-- Grand
insert into kpi.dim_app_daily_metrics(event_day,app_name,transactional_bookings,daily_active_users,montly_active_users,installs)
select 
  a.event_day,
  'Grand Casino' as app_name,
  ifnull(p.bookings,0) as transactional_bookings,
  ifnull(u.daily_active_users,0) as daily_active_users,
  ifnull(m.monthly_active_users,0) as monthly_active_users,
  ifnull(i.installs,0) as installs
from (select thedate as event_day from newapi.dim_date where thedate between sysdate::date-3 and sysdate-1) a  
left join (
	select
	  event_day,
	  count(distinct user_id) as daily_active_users
	from temp_dau
	where app = 'GSN Grand Casino'
	and event_day between sysdate::date-3 and sysdate-1
	group by 1
	order by 1 desc
) u using(event_day)
left join (
	select
	  d2.event_day,
	  count(distinct user_id) as monthly_active_users
	from (select thedate as event_day from newapi.dim_date where thedate between sysdate::date-3 and sysdate-1) d2
	join temp_dau a on a.event_day between d2.event_day - 29 and d2.event_day and a.app = 'GSN Grand Casino'
	where d2.event_day between sysdate::date-3 and sysdate-1
	group by 1
) m using(event_day)
left join (
    select 
      event_day,
      sum(amount_paid_usd) as bookings
    from gsnmobile.events_payments
    where app='GSN Grand Casino'
    group by 1
) p using(event_day)
left join (
    select
      first_seen as event_day,
      count(distinct synthetic_id) as installs
    from gsnmobile.dim_app_installs
    where app='GSN Grand Casino'
    group by 1
) i using(event_day)
order by 1 desc
;

-- WorldWinner
insert into kpi.dim_app_daily_metrics(event_day,app_name,transactional_bookings,daily_active_users,montly_active_users,installs,advertising_revenue)
select 
  d.event_day,
  'GSN Cash Games' as app_name,
  ifnull(r.transactional_bookings,0) as transactional_bookings,
  ifnull(u.daily_active_users,0) as daily_active_users,
  ifnull(m.monthly_active_users,0) as monthly_active_users,
  ifnull(i.installs,0) as installs,
  ifnull(a2.ad_rev,0) as ad_revenue
from (select thedate as event_day from newapi.dim_date where thedate between sysdate::date-3 and sysdate-1) d
left join (
    select 
      event_day,
      sum(nar) as transactional_bookings
    from ww.rpt_ww_daily_daunar
    where event_day >= sysdate::date-3
    group by 1
    order by 1 desc
) r using(event_day) 
left join (
	select
	  event_day,
	  count(distinct user_id) as daily_active_users
	from temp_dau
	where app = 'GSN Cash Games'
	and event_day between sysdate::date-3 and sysdate-1
	group by 1
	order by 1 desc
) u using(event_day)
left join (
	select
	  d2.event_day,
	  count(distinct user_id) as monthly_active_users
	from (select thedate as event_day from newapi.dim_date where thedate between sysdate::date-3 and sysdate-1) d2
	join temp_dau a on a.event_day between d2.event_day - 29 and d2.event_day and a.app = 'GSN Cash Games'
	where d2.event_day between sysdate::date-3 and sysdate-1
	group by 1
) m using(event_day)
left join (
    select
      date(createdate) as event_day,
      count(distinct user_id) as installs
    from ww.dim_users
    group by 1
    order by 1
) i using(event_day)
left join (
	select
	  event_date::date as event_day,
	  sum(revenue) as ad_rev
	from ads.daily_aggregation
	where event_date::date between sysdate::date-3 and sysdate-1
	and vendor <> 'UnityAds'
        and app = 'gsn casino'
        and platform <> 'facebook'
	group by 1
	--order by 1 desc
) a2 using(event_day)
order by 1 desc
;

-- Bash
insert into kpi.dim_app_daily_metrics (event_day,app_name,transactional_bookings,daily_active_users,montly_active_users,installs,advertising_revenue)
select
    d.event_day,
    d.app_name,
    ifnull(a.transactional_bookings,0) as transactional_bookings,
    ifnull(u.daily_active_users,0) as daily_active_users,
    ifnull(m.monthly_active_users,0) as monthly_active_users,
    ifnull(installs,0) as installs
    --ifnull(a2.ad_rev,0) as ad_revenue
from
(
        select 'Slots Bash' as app_name, thedate as event_day from newapi.dim_date where thedate between sysdate::date-3 and sysdate-1
        UNION
        select 'Bingo Bash Canvas', thedate as event_day from newapi.dim_date where thedate between sysdate::date-3 and sysdate-1
        UNION
        select 'Bingo Bash Mobile', thedate as event_day from newapi.dim_date where thedate between sysdate::date-3 and sysdate-1
) d
join (
  select
    event_day,
    case
      when app='SLOTS BASH' then 'Slots Bash'
      when platform='pc' then 'Bingo Bash Canvas'
      when platform in ('kindle',
                        'kindlep',
                        'androidp',
                        'androidap',
                        'androidt',
                        'androidat',
                        'iphone',
                        'ipad') then 'Bingo Bash Mobile'
      else 'Bingo Bash Other' 
    end as app_name,
    sum(gross_bookings) as transactional_bookings,
    sum(new_users) as installs
  from bash.events_daily_metrics
  where date(event_day) >= sysdate::date-3
  and platform<>'gsn_pc'
  group by 1,2
) a using(event_day, app_name)
left join (
	select
	  app as app_name,
	  event_day,
	  count(distinct user_id) as daily_active_users
	from temp_dau
	where app in ('Slots Bash', 'Bingo Bash Canvas', 'Bingo Bash Mobile')
	and event_day between sysdate::date-3 and sysdate-1
	group by 1,2
	--order by 1 desc
) u using(event_day, app_name)
left join (
	select
	  a.app as app_name,
	  d2.event_day,
	  count(distinct user_id) as monthly_active_users
	from (select thedate as event_day from newapi.dim_date where thedate between sysdate::date-3 and sysdate-1) d2
	join temp_dau a on a.event_day between d2.event_day - 29 and d2.event_day and a.app in ('Slots Bash', 'Bingo Bash Canvas', 'Bingo Bash Mobile')
	where d2.event_day between sysdate::date-3 and sysdate-1
	group by 1,2
) m using(event_day, app_name)
order by 1 desc,2
;


-- Idle Gaming
delete from kpi.dim_app_daily_metrics where app_name = 'Fresh Deck Poker'
;
insert into kpi.dim_app_daily_metrics (event_day,app_name,transactional_bookings,daily_active_users,montly_active_users,installs,user_acquisition_spend,advertising_revenue)
select
    d.event_day,
    'Fresh Deck Poker' as app_name,
    ifnull(a.transactional_bookings,0) as transactional_bookings,
    ifnull(u.daily_active_users,0) as daily_active_users,
    ifnull(m.monthly_active_users,0) as monthly_active_users,
    ifnull(a.installs,0) as installs,
    ifnull(a.user_acquisition_spend,0) as user_acquisition_spend,
    ifnull(a.advertising_revenue,0) as advertising_revenue
from (select thedate as event_day from newapi.dim_date where thedate between sysdate::date-3 and sysdate-1) d
join (
	select
		event_day,
		sum(transactional_bookings) as transactional_bookings,
		--sum(daily_active_users)  as daily_active_users,
		sum(previous_day_installs) as installs,
		sum(previous_day_ua_spend) as user_acquisition_spend,
		sum(advertising_revenue) as advertising_revenue
	from idle.events_daily_metrics
	group by 1
) a using(event_day)
left join (
	select
	  app as app_name,
	  event_day,
	  count(distinct user_id) as daily_active_users
	from temp_dau
	where app = 'Fresh Deck Poker'
	and event_day between sysdate::date-3 and sysdate-1
	group by 1,2
	--order by 1 desc
) u using(event_day)
left join (
	select
	  a.app as app_name,
	  d2.event_day,
	  count(distinct user_id) as monthly_active_users
	from (select thedate as event_day from newapi.dim_date where thedate between sysdate::date-3 and sysdate-1) d2
	join temp_dau a on a.event_day between d2.event_day - 29 and d2.event_day and a.app  = 'Fresh Deck Poker'
	where d2.event_day between sysdate::date-3 and sysdate-1
	group by 1,2
) m using(event_day)
order by 1;


-- London Casino Studio
delete from kpi.dim_app_daily_metrics where app_name = 'Mirrorball Slots'
;
insert into kpi.dim_app_daily_metrics (event_day,app_name,transactional_bookings,daily_active_users,installs,user_acquisition_spend,advertising_revenue)
select
    d.event_day,
    'Mirrorball Slots',
    ifnull(transactional_bookings,0) as transactional_bookings,
    ifnull(daily_active_users,0) as daily_active_users,
    ifnull(installs,0) as installs,
    0 as user_acquisition_spend,
    0 as advertising_revenue
from (select thedate as event_day from newapi.dim_date where thedate between sysdate::date-3 and sysdate-1) d
join (
	select
		event_day,
		sum(transactional_bookings) as transactional_bookings,
		sum(daily_active_users)  as daily_active_users,
		sum(installs) as installs,
		0 as user_acquisition_spend,
		0 as advertising_revenue
	from plumbee.daily_metrics
	group by 1
) a using(event_day)
order by 1;

-- Sparcade
insert into kpi.dim_app_daily_metrics(event_day,app_name,transactional_bookings,daily_active_users,monthly_active_users,installs)
select
    rev.event_day,
    'Sparcade' as app_name,
    ifnull(transactional_bookings,0) as transactional_bookings,
    ifnull(u.daily_active_users,0) as daily_active_users,
    ifnull(m.monthly_active_users,0) as monthly_active_users,
    ifnull(inst.installs,0) as installs
from
(
        select date(tourn_close_time) as event_day, sum(user_revenue) as transactional_bookings
        from arena.tournament_entries_results
        where date(tourn_close_time) between sysdate::date-3 and sysdate-1
        and tourn_close_time is not null
        and game_name is not null
        group by 1
) rev
left join
(
        select first_seen as event_day, count(distinct device_id) as installs
        from arena.mobile_devices
        where first_seen between sysdate::date-3 and sysdate-1
        group by 1
) inst using(event_day)
left join (
	select
	  app as app_name,
	  event_day,
	  count(distinct user_id) as daily_active_users
	from temp_dau
	where app = 'Sparcade'
	and event_day between sysdate::date-3 and sysdate-1
	group by 1,2
	--order by 1 desc
) u using(event_day)
left join (
	select
	  a.app as app_name,
	  d2.event_day,
	  count(distinct user_id) as monthly_active_users
	from (select thedate as event_day from newapi.dim_date where thedate between sysdate::date-3 and sysdate-1) d2
	join temp_dau a on a.event_day between d2.event_day - 29 and d2.event_day and a.app  = 'Sparcade'
	where d2.event_day between sysdate::date-3 and sysdate-1
	group by 1,2
) m using(event_day)
order by 1;


-- WoF App
insert into kpi.dim_app_daily_metrics(event_day,app_name,transactional_bookings,daily_active_users,montly_active_users,installs)
select
    a.event_day,
    'Wheel of Fortune Slots' as app_name,
    ifnull(p.bookings,0) as transactional_bookings,
    ifnull(u.daily_active_users,0) as daily_active_users,
    ifnull(m.monthly_active_users,0) as monthly_active_users,
    ifnull(i.installs,0) as installs
from (select thedate as event_day from newapi.dim_date where thedate between sysdate::date-3 and sysdate-1) a
left join (
	select
	  event_day,
	  count(distinct user_id) as daily_active_users
	from temp_dau
	where app = 'WoF App'
	and event_day between sysdate::date-3 and sysdate-1
	group by 1
	order by 1 desc
) u using(event_day)
left join (
	select
	  d2.event_day,
	  count(distinct user_id) as monthly_active_users
	from (select thedate as event_day from newapi.dim_date where thedate between sysdate::date-3 and sysdate-1) d2
	join temp_dau a on a.event_day between d2.event_day - 29 and d2.event_day and a.app = 'WoF App'
	where d2.event_day between sysdate::date-3 and sysdate-1
	group by 1
) m using(event_day)
left join (
-- TEMPORARY HACK!!!: using app_wofs.events_server instead of app_wofs.events_client because amazon purchase are broken with client events
-- (11/03/2016)
    SELECT
      event_day,
      SUM(attr9) AS bookings
    FROM app_wofs.events_server
    WHERE event_type_id = 10
    AND attr24 NOT IN (SELECT hold_back_id FROM app_wofs.blacklist)
    GROUP BY 1
) p using(event_day)
left join (
    select
      first_seen as event_day,
      count(distinct synthetic_id) as installs
    from gsnmobile.dim_app_installs
    where app='WoF App'
    and synthetic_id NOT IN (SELECT hold_back_id FROM app_wofs.blacklist)
    group by 1
) i using(event_day)
order by 1 desc;

commit;
