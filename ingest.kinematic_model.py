#!/usr/bin/env python3

import os
import logging
import asyncio
import aiofiles
import asyncpg
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv


logging.basicConfig(level=logging.INFO)


INSERT_TABLE_QUERY = 'INSERT INTO wallaby.kinematic_model ' \
    '(detection_id,ra,dec,freq,team_release,team_release_kin,vsys_model,e_vsys_model,x_model,e_x_model,y_model,e_y_model,ra_model,e_ra_model,dec_model,e_dec_model,inc_model,e_inc_model,pa_model,e_pa_model,pa_model_g,e_pa_model_g,qflag_model,rad,vrot_model,e_vrot_model,e_vrot_model_inc,rad_sd,sd_model,e_sd_model,sd_fo_model,e_sd_fo_model_inc)' \
    'VALUES ' \
    '($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32) ' \
    'ON CONFLICT DO NOTHING;'


INSERT_PRODUCT_QUERY = 'INSERT INTO wallaby.wkapp_product ' \
    '(kinematic_model_id, baroloinput, barolomod, barolosurfdens, fullresmodcube, fullresproccube, fatinput, fatmod, diagnosticplot, diffcube, procdata, modcube) ' \
    'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) ' \
    'ON CONFLICT DO NOTHING;'


async def _get_file_bytes(path: str, mode: str = 'rb'):
    buffer = []
    if not os.path.isfile(path):
        return b''
    async with aiofiles.open(path, mode) as f:
        while True:
            buff = await f.read()
            if not buff:
                break
            buffer.append(buff)
        if 'b' in mode:
            return b''.join(buffer)
        else:
            return ''.join(buffer)


async def insert_row(conn, run_name, row):
    """Insert row of CSV into kinematic model table

    """
    detection_name = row['name']
    logging.info(f'Inserting detection {detection_name}')
    detection = await conn.fetchrow(
        f'SELECT detection.id FROM (wallaby.detection LEFT JOIN wallaby.run ON detection.run_id=run.id) WHERE (detection.source_name=$1 AND run.name=$2)',
        detection_name,
        run_name
    )
    if detection is None:
        logging.error(f'No detection found with source name {detection_name}. Skipping.')
        return 0

    logging.info(f'Detection id: {detection["id"]}')

    # Insert
    res = await conn.execute(
        INSERT_TABLE_QUERY,
        int(detection['id']),
        row['ra'], row['dec'], row['freq'], row['team_release'], row['team_release_kin'], row['Vsys_model'], row['e_Vsys_model'],
        row['X_model'], row['e_X_model'], row['Y_model'], row['e_Y_model'], row['RA_model'], row['e_RA_model'], row['DEC_model'],
        row['e_DEC_model'], row['Inc_model'], row['e_Inc_model'], row['PA_model'], row['e_PA_model'], row['PA_model_g'], row['e_PA_model_g'],
        row['QFlag_model'], row['Rad'], row['Vrot_model'], row['e_Vrot_model'], row['e_Vrot_model_inc'], row['Rad_SD'], row['SD_model'],
        row['e_SD_model'], row['SD_FO_model'], row['e_SD_FO_model_inc']
    )
    logging.info(res)
    return res


async def insert_products(conn, source_name, run_name, subdir):
    """Insert products into wkapp_product table

    """
    detection = await conn.fetchrow(
        f'SELECT detection.id FROM (wallaby.detection LEFT JOIN wallaby.run ON detection.run_id=run.id) WHERE (detection.source_name=$1 AND run.name=$2)',
        source_name,
        run_name
    )
    if detection is None:
        logging.error(f'No detection found with source name {source_name} in run {run_name}. Skipping.')
        return 0
    logging.info(f'Inserting products for {source_name} (run={run_name}, id={detection["id"]})')
    kinematic_model = await conn.fetchrow(
        f'SELECT id FROM wallaby.kinematic_model WHERE detection_id=$1',
        int(detection['id'])
    )
    logging.info(f'Inserting into kinematic model with id={kinematic_model["id"]}')

    # Barolo files
    barolofiles = Path(os.path.join(str(subdir), 'FitsUsedForAveraging', 'Barolo-No_Ini'))
    barolo_input_file = list(barolofiles.glob('*BaroloInput.txt'))[0]
    barolo_mod_file = list(barolofiles.glob('*BaroloMod.txt'))[0]
    barolo_surfdens_file = list(barolofiles.glob('*BaroloSurfDens.txt'))[0]

    # FAT
    fatfiles = Path(os.path.join(str(subdir), 'FitsUsedForAveraging', 'FAT'))
    fat_input_file = list(fatfiles.glob('*FATInput.txt'))[0]
    fat_mod_file = list(fatfiles.glob('*FATMod.txt'))[0]

    # Full Res
    fullresfiles = Path(os.path.join(str(subdir), 'FullResolution'))
    fullresmodcube_file = list(fullresfiles.glob('*FullResModCube.fits'))[0]
    fullresproccube_file = list(fullresfiles.glob('*FullResProcData.fits'))[0]

    # Other products
    diagnostic_plot_file = list(subdir.glob('*DiagnosticPlot.png'))[0]
    diffcube_file = list(subdir.glob('*DiffCube.fits'))[0]
    procdata_file = list(subdir.glob('*ProcData.fits'))[0]
    modcube_file = list(subdir.glob('*ModCube.fits'))[0]

    # As bytes
    barolo_input = await _get_file_bytes(barolo_input_file)
    barolo_mod = await _get_file_bytes(barolo_mod_file)
    barolo_surfdens = await _get_file_bytes(barolo_surfdens_file)
    fullresmodcube = await _get_file_bytes(fullresmodcube_file)
    fullresproccube = await _get_file_bytes(fullresproccube_file)
    fat_input = await _get_file_bytes(fat_input_file)
    fat_mod = await _get_file_bytes(fat_mod_file)
    diagnostic_plot = await _get_file_bytes(diagnostic_plot_file)
    diffcube = await _get_file_bytes(diffcube_file)
    procdata = await _get_file_bytes(procdata_file)
    modcube = await _get_file_bytes(modcube_file)

    # Insert
    res = await conn.execute(
        INSERT_PRODUCT_QUERY,
        kinematic_model['id'],
        barolo_input, barolo_mod, barolo_surfdens, fullresmodcube, fullresproccube, fat_input, fat_mod, diagnostic_plot, diffcube, procdata, modcube
    )
    logging.info(res)
    return


async def main():
    # Establish database connection
    load_dotenv('/Users/she393/Documents/config/wallaby/database.env')
    host = os.getenv('DATABASE_HOST')
    user = os.getenv('DATABASE_USER')
    database = os.getenv('DATABASE_NAME')
    password = os.getenv('DATABASE_PASSWORD')
    conn = await asyncpg.connect(dsn=None, host=host, database=database, user=user, password=password)

    # Target upload
    run_name = 'phase2_highres'
    basedir = '/Users/she393/Documents/wallaby_pdr/PDR2_HR_KinematicModels/Wallaby_Vela_HR_DR1_KinematicModels_v1'

    # Ingest kinematic model
    csvfile = os.path.join(basedir, 'Wallaby_Vela_HR_DR1_KinematicModels_v1.csv')
    table = pd.read_csv(csvfile)
    for idx, row in table.iterrows():
        async with conn.transaction():
            await insert_row(conn, run_name, row)

    # Insert products
    product_files = [x for x in Path(basedir).iterdir() if x.is_dir()]
    for subdir in product_files:
        logging.info(subdir)
        detection_name = ' '.join(subdir.name.split('_')[0:2])
        logging.info(detection_name)
        try:
            async with conn.transaction():
                await insert_products(conn, detection_name, run_name, subdir)
        except Exception as e:
            logging.error(e)
            logging.error(f'Unexpected directory format. Skipping {subdir}')

    return 0


if __name__ == '__main__':
    asyncio.run(main())