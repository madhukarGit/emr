def to_files(df, tgt_dir, file_format):
    df.coalesce(16). \
        write. \
        partitionBy('year', 'month', 'dayofmonth'). \
        mode('append'). \
        format(file_format). \
        save(tgt_dir)
        
# export ENVIRON=DEV
# export SRC_DIR=s3://ghvemractivitydata/landing
# export SRC_FILE_PATTERN=2021-01-13
# export SRC_FILE_FORMAT=json
# export TGT_DIR=s3://ghvemractivitydata/write
# export TGT_FILE_FORMAT=parquet