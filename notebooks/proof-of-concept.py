#!/usr/bin/env python
# coding: utf-8

# # Proof of concept: Virtualizing CMIP6 netcdf files

# In[1]:


# # install virtualizarr
# !pip install git+https://github.com/jbusecke/VirtualiZarr.git@esgf-cmip-test


# In[ ]:


from tqdm.auto import tqdm
from virtualizarr import open_virtual_dataset
from virtualizarr.kerchunk import FileType
import xarray as xr

# In[5]:


# data is located on public s3 (more info: https://pangeo-data.github.io/pangeo-cmip6-cloud/overview.html#netcdf-data-overview)
paths = [
#'http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/DKRZ/MPI-ESM1-2-HR/ssp126/r1i1p1f1/Amon/tas/gn/v20190710/tas_Amon_MPI-ESM1-2-HR_ssp126_r1i1p1f1_gn_201501-201912.nc',
#'http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/DKRZ/MPI-ESM1-2-HR/ssp126/r1i1p1f1/Amon/tas/gn/v20190710/tas_Amon_MPI-ESM1-2-HR_ssp126_r1i1p1f1_gn_202001-202412.nc',
#'http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/DKRZ/MPI-ESM1-2-HR/ssp126/r1i1p1f1/Amon/tas/gn/v20190710/tas_Amon_MPI-ESM1-2-HR_ssp126_r1i1p1f1_gn_202501-202912.nc'
#]


     'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_188101-189012.nc',
#     'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_186101-187012.nc',
     'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_189101-190012.nc',
]
    # 'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_190101-191012.nc',
    # 'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_191101-192012.nc',
    # 'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_192101-193012.nc',
    # 'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_193101-194012.nc',
    # 'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_194101-195012.nc',
    # 'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_195101-196012.nc',
    # 'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_196101-197012.nc',
    # 'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_197101-198012.nc',
    # 'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_198101-199012.nc',
    # 'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_199101-200012.nc',
    # 'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_200101-201012.nc',
    # 'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_201101-201412.nc'



# In[ ]:


# load virtual datasets in serial
vds_list = []
for f in tqdm(paths):
    vds = open_virtual_dataset(f, filetype=FileType.netcdf4, indexes={}, reader_options={})
    vds_list.append(vds)


# In[ ]:


combined_vds = xr.combine_nested(vds_list, concat_dim=['time'], coords='minimal', compat='override')


# In[ ]:


combined_vds.virtualize.to_kerchunk('combined_full.json', format='json')


# ## Read from local json
# If you executed all steps above, you should be able to execute this cell.

