import descarteslabs as dl
import os
import sys
from uuid import uuid4
from shapely.geometry import shape
import warnings
warnings.filterwarnings("ignore")

def download_tile(args):
        tile, tmpdir, product_id, bands, start_datetime, end_datetime = args
        scenes, ctx = dl.scenes.search(
            tile, 
            product_id, 
            start_datetime=start_datetime, 
            end_datetime=end_datetime, 
            limit=None)
        scenes.download_mosaic(bands, ctx, dest=os.path.join(tmpdir,f'{tile.key}.tif'))

    
def download_async(AOI, product_id, resolution, destination, start_datetime=None, end_datetime=None, tile_size=None, bands=None, run_async=False, job_id=None):
    """
        Method to mosaic scene data and download whole or in tiles
    """
    import rasterio
    import descarteslabs as dl
    import os
    from rasterio.merge import merge
    from rasterio.mask import mask
    import pyproj
    from tqdm import tqdm_notebook as tqdm
    from multiprocessing import Pool
    from itertools import repeat
    from functools import partial
    import glob
    from tempfile import TemporaryDirectory, TemporaryFile
    from shapely.ops import transform
    from shapely.geometry import shape
    from dlutils.downloader import download_tile

    if bands is None:
        bands = [
            band.name 
            for bid, band in 
                dl.metadata.get_bands_by_product(product_id).items()]

    if tile_size is not None:
        """
            Create tiles, split up the download, and merge the result 
                - required for downloads larger than 5GB, sometimes smaller
        """
        tiles = dl.scenes.DLTile.from_shape(AOI, resolution, tilesize=tile_size, pad=0)
        with TemporaryDirectory() as tmpdir:

            with Pool() as pool:
                
                list(tqdm(pool.imap_unordered(download_tile, zip(tiles, repeat(tmpdir), repeat(product_id), repeat(bands), repeat(start_datetime), repeat(end_datetime))), desc=destination, total=len(tiles)))

            tile_tifs = [] 
            tile_files = glob.glob(os.path.join(tmpdir, '*.tif'))
            for infile in tile_files:
                src = rasterio.open(infile)
                tile_tifs.append(src)

            print('Mosaicking tiles')
            mosaic, out_trans = merge(tile_tifs)

            out_meta = src.meta.copy()
            out_meta.update({"driver": "GTiff",
                             "height": mosaic.shape[1],
                             "width": mosaic.shape[2],
                             "transform": out_trans,
                             })
            print('Clipping to AOI')


            with rasterio.open(open(os.path.join(tmpdir, 'mosaic.tif'), "wb"), "w", **out_meta) as mosaic_ds:
                mosaic_ds.write(mosaic)  


                prj = partial(
                    pyproj.transform,
                    pyproj.Proj(init='epsg:4326'), 
                    pyproj.Proj(init=tiles[0].crs)) 

                AOI = transform(prj, shape(AOI))  # apply projection

                clipped, out_trans = mask(mosaic_ds, [AOI.__geo_interface__], crop=True)
                out_meta = src.meta.copy()
                out_meta.update({"driver": "GTiff",
                             "height": clipped.shape[1],
                             "width": clipped.shape[2],
                             "transform": out_trans})
                if run_async:
                    destination = destination.split('/')[-1]
                with rasterio.open(open(destination, "wb"), "w", **out_meta) as dest:
                    dest.write(clipped)
                
    else:
        scenes, ctx = dl.scenes.search(dl.scenes.AOI(AOI, resolution), product_id, limit=None)
        scenes.download_mosaic(bands, ctx, dest=destination)

    if run_async:
        with open(destination, 'wb+') as f:
            dl.Storage().set_file(destination, f)

    return destination


def download(AOI, product_id, resolution=10., start_datetime=None, end_datetime=None, bands=None, destination='dl.tif', tile_size=None, run_async=False):
    """
        Download GeoTiff mosaics as tiles or single images 

        Parameters
        ----------
        AOI: GeoJSON Geometry, required
        product_id: str, required
        resolution: float, optional
            Resolution in meters
        bands: list, optional default: None
            List of band names to include, None for all bands
        destination: str, required default: dl.tif
            Output file to write into
        tile_size: int, optional default: None
            Tile size in pixels to break the download into. Defaults to None - no tiling
        run_async: bool, optional default: False
            Run asyncronously on DL tasks, helpful if many tiles are required an Result is passed 
            through DL Storage to the client and stored behind a key with the same name as the output path
            
        Returns
        -------
        path: str
            Local path to file downloaded
    """

    if run_async: 
        async_func = dl.Tasks().create_function(
            'dlutils.downloader.download_async',
            image=f'us.gcr.io/dl-ci-cd/images/tasks/public/py3.7/default:v2019.08.08-7-g062b0653',
            name=f'Downloading {product_id}',
            cpu=8,
            maximum_concurrency=1,
            mem='32Gi',
            task_timeout=60*120,
            include_modules=['dlutils'],
            requirements=['tqdm']
        ) 
        job_id = uuid4().hex
    else:
        async_func = download_async
        job_id=None
    if run_async:
        print(f"""
        Assembling {destination.split('/')[-1]} on DLTasks ... this will take a few minutes.""")
    result = async_func(AOI, product_id, resolution, destination, start_datetime=start_datetime, end_datetime=end_datetime, tile_size=tile_size, bands=bands, run_async=run_async, job_id=job_id)
    
    if run_async:
        if result.status == 'SUCCESS' and result.result is not None:
            print(f'Downloading {result.result} to {destination}')
            dl.Storage().get_file(result.result, destination)
            dl.Storage().delete(result.result)
            print('Done.')
        else:
            print(result.status)
        dl.Tasks().delete_group_by_id(async_func.group_id)
    else:
        print(f'Saved to {result}')  