import shapefile
from sys import argv
import requests
import os
from json import dumps
import netCDF4 as nc


def run_program(shp, data_dir, output_fpath):
    geom = extract_geometry_from_shapefile(shp)
    code = get_crs_code(shp)

    if code:
        esri_geom_json = {"rings": geom['coordinates'], "spatialReference": {"wkid": code}}

        if esri_geom_json:
            grid_cells_indices_list = get_grid_cells_indices_list(dumps(esri_geom_json))

            if grid_cells_indices_list:
                subset_and_combine_files(data_dir, grid_cells_indices_list, output_fpath)
                print "DONE"

    # geojson_str = '{"rings":[[[-97, 32], [-97, 33], [-96, 33], [-97,32]]],"spatialReference":{"wkid":4326}}'
    # grid_cells_indices_list = get_grid_cells_indices_list(geojson_str)
    #
    # if grid_cells_indices_list:
    #     subset_and_combine_files("/home/drew/Desktop/nwm", grid_cells_indices_list, "/home/drew/Desktop/nwm/output")
    #     print "DONE"

def subset_and_combine_files(data_dir, grid_cells_indices_list, output_fpath):
    temp_dir = get_temp_dir()

    unique_x_indices, unique_y_indices, index_mapping = get_unique_indices_and_mapping(grid_cells_indices_list)

    num_files = len(os.listdir(data_dir))
    file_count = 1
    for f in os.listdir(data_dir):
        if not f.endswith('.nc'):
            file_count += 1
            continue
        print "Subsetting file %s of %s" % (file_count, num_files)
        file_count += 1

        orig_fpath = os.path.join(data_dir, f)
        print orig_fpath
        subset_fpath = os.path.join(temp_dir, f)
        in_nc = nc.Dataset(orig_fpath, mode='r')

        with nc.Dataset(subset_fpath, mode='w', format=in_nc.data_model) as out_nc:
            out_nc.setncatts({k: in_nc.getncattr(k) for k in in_nc.ncattrs()})
            for name, dim in in_nc.dimensions.iteritems():
                length = len(unique_x_indices) if name == 'x' else len(unique_y_indices)
                out_nc.createDimension(name, length)

            for name, var in in_nc.variables.iteritems():
                out_var = out_nc.createVariable(name, var.datatype, var.dimensions)
                attributes = {k: var.getncattr(k) for k in var.ncattrs()}
                out_var.setncatts(attributes)

                if name == 'x':
                    for x_index in unique_x_indices:
                        new_index = index_mapping['x'][x_index]
                        out_var[new_index] = var[x_index]
                elif name == 'y':
                    for y_index in unique_y_indices:
                        new_index = index_mapping['y'][y_index]
                        out_var[new_index] = var[y_index]
                else:
                    if len(var.dimensions) == 2:
                        for grid_cells_indices in grid_cells_indices_list:
                            x_index_old = grid_cells_indices[0]
                            y_index_old = grid_cells_indices[1]
                            x_index_new = index_mapping['x'][x_index_old]
                            y_index_new = index_mapping['y'][y_index_old]
            
                            out_var[y_index_new, x_index_new] = var[y_index_old][x_index_old]

    combine_files(temp_dir, output_fpath)
        
    remove_temp_files()


def extract_geometry_from_shapefile(shp):
    # shp_reader = shapefile.Reader(shp=open(shp), dbf=open(shp.replace('shp', 'dbf')),
    #                               shx=open(shp.replace('shp', 'shx')))
    shp_reader = shapefile.Reader(shp)
    shape_records = shp_reader.shapeRecords()
    #if len(shape_records) > 1:
    #    raise Exception
    geom = shape_records[0].shape.__geo_interface__

    return geom


def get_grid_cells_indices_list(esri_geom_json_str):
    url = "http://geoserver.byu.edu/arcgis/rest/services/NWM/grid/MapServer/0/query"
    params = {
        'geometry': esri_geom_json_str,
        'geometryType': 'esriGeometryPolygon',
        'returnGeometry': 'false',
        'spatialRel': 'esriSpatialRelIntersects',
        'outFields': 'south_north, west_east',
        'f': 'json'
    }

    r_json = requests.post(url, data=params).json()

    grid_cells_json = r_json['features']
    grid_cells_indices_list = []

    for grid_cell_json in grid_cells_json:
        attributes = grid_cell_json['attributes']
        grid_cells_indices_list.append([attributes['west_east'], attributes['south_north']])

    return grid_cells_indices_list


def get_unique_indices_and_mapping(grid_indices_list):
    unique_x_indices = []
    unique_y_indices = []
    orig_to_new_mapping = {
        'x': {},
        'y': {}
    }
    for grid_indices in grid_indices_list:
        if grid_indices[0] not in unique_x_indices:
            unique_x_indices.append(int(grid_indices[0]))
        if grid_indices[1] not in unique_y_indices:
            unique_y_indices.append(int(grid_indices[1]))

    unique_x_indices.sort()
    unique_y_indices.sort()
    for i, x in enumerate(unique_x_indices):
        orig_to_new_mapping['x'][x] = i

    for i, y in enumerate(unique_y_indices):
        orig_to_new_mapping['y'][y] = i

    return unique_x_indices, unique_y_indices, orig_to_new_mapping


def get_temp_dir():
    temp_dir = 'tmp-nwm-data'

    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    return temp_dir


def remove_temp_files():
    temp_dir = 'tmp-nwm-data'
    
    os.system('rm -rf %s' % temp_dir)


def combine_files(nc_files_dir, output_file_path):
    print "Combining all files"
    os.system('ncecat -h %s/* %s' % (nc_files_dir, output_file_path))


def get_crs_code(shp_file):
    crs_endpoint = 'http://prj2epsg.org/search.json'
    code = None
    crs_is_unknown = True

    with open(shp_file.replace('shp', 'prj')) as f:
        crs = f.read()

    params = {
        'mode': 'wkt',
        'terms': crs
    }

    try:
        while crs_is_unknown:
            r = requests.get(crs_endpoint, params=params)
            print r.url
            if '50' in str(r.status_code):
                raise Exception
            elif r.status_code == 200:
                response = r.json()
                if 'errors' in response:
                    errs = response['errors']
                    if 'Invalid WKT syntax' in errs:
                        err = errs.split(':')[2]
                        if err and 'Parameter' in err:
                            crs_param = err.split('"')[1]
                            rm_indx_start = crs.find(crs_param)
                            rm_indx_end = None
                            sub_str = crs[rm_indx_start:]
                            counter = 0
                            check = False
                            for i, c in enumerate(sub_str):
                                if c == '[':
                                    counter += 1
                                    check = True
                                elif c == ']':
                                    counter -= 1
                                    check = True
                                if check:
                                    if counter == 0:
                                        rm_indx_end = i + rm_indx_start + 1
                                        break
                            crs = crs[:rm_indx_start] + crs[rm_indx_end:]
                            if ',' in crs[:-4]:
                                i = crs.rfind(',')
                                crs = crs[:i] + crs[i + 1:]
                            params['terms'] = crs
                        else:
                            break
                    else:
                        break
                else:
                    crs_is_unknown = False
                    code = response['codes'][0]['code']
            else:
                params['mode'] = 'keywords'
                continue
    except Exception as e:
        print str(e)
        print 'Projection was not recognized. Script terminated.'

    return code


#run_program(argv[1], argv[2], argv[3])
run_program("/home/drew/Desktop/state/utah.shp", "/home/drew/Desktop/nwm", "/home/drew/Desktop/nwm/output")


