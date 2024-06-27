from tqdm.auto import tqdm
from virtualizarr import open_virtual_dataset
import xarray as xr
from dask.diagnostics import ProgressBar

import sys
import os


DATA_NODE_PREFIX="http://aims3.llnl.gov/thredds/fileServer/css03_data"
LOCAL_PREFIX="/p/css03/esgf_publish"
OUT_LOC="/p/user_pub/work/vzarr"

fmt = sys.argv[1]
rel_path = ""
dsid =""

if fmt == "-path":
    rel_path = sys.argv[2]
    dsid = rel_path.replace("/",".")
else:
    dsid = sys.argv[2]
    rel_path = dsid.replace('.','/')



urls = [f"{DATA_NODE_PREFIX}/{rel_path}/{fn}" for fn in os.listdir(f"{LOCAL_PREFIX}/{rel_path}")]
    

json_filename = f"{OUT_LOC}/{dsid}.json"




# load virtual datasets in serial
vds_list = []
for url in tqdm(urls):
    vds = open_virtual_dataset(
        url, indexes={}, reader_options={}
    )  # reader_options={} is needed for now to circumvent a bug in https://github.com/TomNicholas/VirtualiZarr/pull/126
    vds_list.append(vds)

combined_vds = xr.combine_nested(
    vds_list,
    concat_dim=["time"],
    coords="minimal",
    compat="override",
    combine_attrs="drop_conflicts",
)
combined_vds.virtualize.to_kerchunk(json_filename, format="json")

## test load and print the the mean of the output
# print(f"Loading the mean of the virtual dataset from {json_filename=}")

# ds = xr.open_dataset(
#     json_filename, 
#     engine='kerchunk',
#     chunks={},
# )
# print(f"Dataset before mean: {ds}")
# with ProgressBar():
#     ds_mean = ds.mean().load()
# print(ds_mean) 
