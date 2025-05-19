-- ./docker/init_user_actions_table.sql

CREATE TABLE IF NOT EXISTS public.user_actions (
  user_id   TEXT,
  username      TEXT,
  action    TEXT,
  timestamp TEXT,
  subscribe INTEGER
);
