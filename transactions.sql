with trnsactions_with_diff as ( 
	SELECT 
	*, 
	LEAD(dt) over (PARTITION by sender order by dt) as next_dt,
	(JULIANDAY(LEAD(dt, 1, dt) over (PARTITION by sender order by dt)) - JULIANDAY(dt))*1440 as diff_datetime,
	ROW_NUMBER() OVER (order by sender, dt) as rn
	from transactions t
	order by sender, dt
),
transaction_series as (
	select 
	*,
	lead(amount, 1, 0) over (PARTITION by sender order by dt) as next_amount
	from transactions_with_diff twd
	where diff_datetime <= 60 
)
select 
	t.sender,
	min(t.dt) sequence_start,
	max(t.next_dt) sequence_end,
	count(*) + 1 transactions_count,
	sum(t.amount) transactions_amount
from transaction_series t
where next_dt is not NULL 
group by sender
having sum(t.amount) >= 150
order by sender, sequence_starta
