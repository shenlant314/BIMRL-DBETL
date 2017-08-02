create type point3d as (
		  x	double precision,
		  y	double precision,
		  z	double precision
);
grant usage on type point3d to public;

/*
create type coordsystem as (
		  xaxis point3d,
		  yaxis point3d,
		  zaxis point3d,
		  origin point3d
);
grant usage on type coordsystem to public;
*/

create type geom3dtype as enum (
    'geompoint3d', 'geompoint3dset', 'geomline3d', 'geompolyline3d', 'geomface3d', 
    'geomsurface3d', 'geomsolid3d', 'geombox3d', 'geomtraingulatedfaceset3d'
);
grant usage on type geom3dtype to public;

create table bimrl_federatedmodel (
		  modelname	varchar(256) not null,
		  projectname varchar(256) not null,
		  projectnumber varchar(256) not null, 
		  projectid varchar(22),
		  federatedid serial not null,
		  worldbbox point3d[2],
		  maxoctreelevel int check (maxoctreelevel > 1 and maxoctreelevel < 19),
		  lastupdatedate date,
		  owner	varchar(32) not null default user,
		  dbconnection varchar(128),
		  primary key (modelname, projectnumber, projectname)
);
grant select,insert,update,delete on bimrl_federatedmodel to public;

insert into bimrl_federatedmodel (modelname, projectname, projectnumber, federatedid) values ('dummy project', 'project name', 'project number', 0);

create table bimrl_objecthierarchy (
		  ifcschemaver varchar(32) not null,
		  elementtype varchar(64) not null, 
		  elementsubtype varchar(64) not null, 
		  abstract boolean, 
		  levelsremoved integer
);
grant select on bimrl_objecthierarchy to public;

create index idx1_bimrl_objhier on bimrl_objecthierarchy (ifcschemaver, elementtype);
create index idx2_bimrl_objhier on bimrl_objecthierarchy (ifcschemaver, elementsubtype);

create table colordict (
		  elementtype varchar(64) not null, 
		  ambientintensity real, 
		  diffusecolorred int check (diffusecolorred < 256 and diffusecolorred >= 0), 
		  diffusecolorgreen int check (diffusecolorgreen < 256 and diffusecolorgreen >= 0), 
		  diffusecolorblue int check (diffusecolorblue < 256 and diffusecolorblue >= 0), 
		  emissivecolorred int check (emissivecolorred < 256 and emissivecolorred >= 0), 
		  emissivecolorgreen int check (emissivecolorgreen < 256 and emissivecolorgreen >= 0), 
		  emissivecolorblue int check (emissivecolorblue < 256 and emissivecolorblue >= 0), 
		  shininess real, 
		  specularcolorred int check (specularcolorred < 256 and specularcolorred >= 0), 
		  specularcolorgreen int check (specularcolorgreen < 256 and specularcolorgreen >= 0), 
		  specularcolorblue int check (specularcolorblue < 256 and specularcolorblue >= 0), 
		  transparency real 
);
grant select on colordict to public;
