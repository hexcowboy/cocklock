pub static PG_TABLE_QUERY: &str = "
create table if not exists $1 (
    client_id uuid not null,
    lock_name text not null unique,
    expires timestamp
);

create or replace function _lock_reap() returns trigger as $$
    begin
        delete from $1
        where
            $1.expires is not null
        	and now() > now() - $1.expires
    end;
$$ language plpgsql;

create or replace trigger _lock_reap_trigger
    before insert or update
    on $1
    execute function _lock_reap();
";

pub static PG_LOCK_QUERY: &str = "
insert into $1 (client_id, lock_name, timeout)
select $2, $3, 'interval ' || $4 || ' milliseconds';
";

pub static PG_EXTEND_QUERY: &str = "
update $1
set timeout = now() + 'interval ' || $2 || ' milliseconds'
where
    client_id = $3
    and lock_name = $4;
";

pub static PG_UNLOCK_QUERY: &str = "
delete from $1
where
    client_id = $2
    and lock_name = $3;
";

pub static PG_CLEAN_UP: &str = "
drop trigger if exists _lock_reap_trigger on $1;
drop function if exists _lock_reap();
drop table if exists $1;
";
