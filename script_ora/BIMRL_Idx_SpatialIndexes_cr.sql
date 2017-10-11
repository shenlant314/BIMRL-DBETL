//Drop index IDX_BIMRLELEM_GEOM_&1;
// Create Index IDX_BIMRLELEM_GEOM_&1 on BIMRL_ELEMENT_&1
//	(GEOMETRYBODY) INDEXTYPE is MDSYS.SPATIAL_INDEX PARAMETERS ('sdo_indx_dims=3');

//Drop index IDX_BIMRLELEM_GEOMBB_&1;
// Create Index IDX_BIMRLELEM_GEOMBB_&1 on BIMRL_ELEMENT_&1
//	(GeometryBody_BBOX) INDEXTYPE is MDSYS.SPATIAL_INDEX PARAMETERS ('sdo_indx_dims=3');

//Drop index IDX_BIMRLELEM_GEOMBBC_&1;
// Create Index IDX_BIMRLELEM_GEOMBBC_&1 on BIMRL_ELEMENT_&1
//   (GeometryBody_BBOX_CENTROID) INDEXTYPE is MDSYS.SPATIAL_INDEX PARAMETERS ('sdo_indx_dims=3');

//Drop index IDX_BIMRLELEM_GEOMFP_&1;
// Create Index IDX_BIMRLELEM_GEOMFP_&1 on BIMRL_ELEMENT_&1
//   (GeometryFootprint) INDEXTYPE is MDSYS.SPATIAL_INDEX PARAMETERS ('sdo_indx_dims=3');

//Drop index IDX_BIMRLELEM_GEOMAX_&1;
// Create Index IDX_BIMRLELEM_GEOMAX_&1 on BIMRL_ELEMENT_&1
//   (GeometryAxis) INDEXTYPE is MDSYS.SPATIAL_INDEX PARAMETERS ('sdo_indx_dims=3');

alter table bimrl_spatialindex_&1 enable primary key;
create index idx_spatial_cellid_&1 on bimrl_spatialindex_&1 (cellid);

create index ixminb_spatialindex_&1 on bimrl_spatialindex_&1 (xminbound);
create index iyminb_spatialindex_&1 on bimrl_spatialindex_&1 (yminbound);
create index izminb_spatialindex_&1 on bimrl_spatialindex_&1 (zminbound);
create index ixmaxb_spatialindex_&1 on bimrl_spatialindex_&1 (xmaxbound);
create index iymaxb_spatialindex_&1 on bimrl_spatialindex_&1 (ymaxbound);
create index izmaxb_spatialindex_&1 on bimrl_spatialindex_&1 (zmaxbound);

