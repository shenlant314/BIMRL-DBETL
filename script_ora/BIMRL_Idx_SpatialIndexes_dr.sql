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

alter table bimrl_spatialindex_&1 disable primary key;
drop index idx_spatial_cellid_&1;

drop index ixminb_spatialindex_&1;
drop index iyminb_spatialindex_&1;
drop index izminb_spatialindex_&1;
drop index ixmaxb_spatialindex_&1;
drop index iymaxb_spatialindex_&1;
drop index izmaxb_spatialindex_&1;
