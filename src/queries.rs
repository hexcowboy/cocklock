pub static PG_TABLE_QUERY: &str = "
create table if not exists TABLE_NAME (
    client_id uuid not null,
    lock_name text not null unique,
    expires_at timestamp
);

create or replace function _lock_reap() returns trigger as $$
    begin
        delete from TABLE_NAME
        where
            TABLE_NAME.expires_at is not null
        	and now() > now() - TABLE_NAME.expires_at;
        return null;
    end;
$$ language plpgsql;

create or replace trigger _lock_reap_trigger
    before insert or update
    on TABLE_NAME
    execute function _lock_reap();
";

pub static PG_LOCK_QUERY: &str = "
insert into TABLE_NAME (client_id, lock_name, expires_at)
select $1, $2, now() + 'interval ' || $3 || ' milliseconds'
on conflict (lock_name) do update
    set expires_at = now() + 'interval ' || $3 || ' milliseconds'
    where
        client_id = exlcluded.client_id
        and lock_name = exlcluded.lock_name;
";

pub static PG_UNLOCK_QUERY: &str = "
delete from TABLE_NAME
where
    client_id = $1
    and lock_name = $2;
";

pub static PG_CLEAN_UP: &str = "
drop trigger if exists _lock_reap_trigger on TABLE_NAME;
drop function if exists _lock_reap();
drop table if exists TABLE_NAME;
";
