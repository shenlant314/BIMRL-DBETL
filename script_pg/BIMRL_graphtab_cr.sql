create table &1_node$ (
	node_id				integer not null,
	node_name			varchar(200),
	node_type			varchar(200),
	active				varchar(1),
	partition_id		integer,
	hierarchy_level	integer,
	parent_node_id		integer,
	primary key (node_id)
);
grant select, update, insert, delete on &1_node$ to public;

create table &1_link$ (
	link_id				integer not null,
	link_name			varchar(200),
	start_node_id		integer not null,
	end_node_id			integer not null,
	link_type			varchar(200),
	active				varchar(1),
	link_level			integer,
	cost					integer,
	parent_link_id		integer,
	primary key (link_id)
);
grant select, update, insert, delete on &1_link$ to public;

