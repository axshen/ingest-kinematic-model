#!/usr/bin/env python3

import os
import logging
import asyncio
import aiofiles
import asyncpg
import pandas as pd
from dotenv import load_dotenv


logging.basicConfig(level=logging.INFO)


INSERT_TABLE_QUERY = 'INSERT INTO wallaby.kinematic_model ' \
    '(detection_id,ra,dec,freq,team_release,team_release_kin,vsys_model,e_vsys_model,x_model,e_x_model,y_model,e_y_model,ra_model,e_ra_model,dec_model,e_dec_model,inc_model,e_inc_model,pa_model,e_pa_model,pa_model_g,e_pa_model_g,qflag_model,rad,vrot_model,e_vrot_model,e_vrot_model_inc,rad_sd,sd_model,e_sd_model,sd_fo_model,e_sd_fo_model_inc)' \
    'VALUES ' \
    '($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32) ' \
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
    csvfile = os.path.join(basedir, 'Wallaby_Vela_HR_DR1_KinematicModels_v1.csv')
    product_files = None

    # Ingest kinematic model
    table = pd.read_csv(csvfile)
    for idx, row in table.iterrows():
        async with conn.transaction():
            await insert_row(conn, run_name, row)

    # Insert products

    return


if __name__ == '__main__':
    asyncio.run(main())