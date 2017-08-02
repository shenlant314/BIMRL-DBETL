drop table usergeom_geometry_bck;
create table usergeom_geometry_bck as select * from usergeom_geometry;
truncate table usergeom_geometry;
drop table usergeom_topo_face_bck;
create table usergeom_topo_face_bck as select * from usergeom_topo_face;
truncate table usergeom_topo_face;
drop table usergeom_spatialindex_bck;
create table usergeom_spatialindex_bck as select * from usergeom_spatialindex;
truncate table usergeom_spatialindex;
