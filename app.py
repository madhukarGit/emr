
import os
from util import get_spark_session
from read import from_files
from process import transform
from write import to_files

def main():
    env = os.environ.get('ENVIRON')
    src_dir = os.environ.get('SRC_DIR')
    file_pattern = f"{os.environ.get('SRC_FILE_PATTERN')}-*"
    src_file_format = os.environ.get('SRC_FILE_FORMAT')
    tgt_dir = os.environ.get('TGT_DIR')
    tgt_file_format = os.environ.get('TGT_FILE_FORMAT')
    spark = get_spark_session(env, 'GitHub Activity - Reading Data')
    df = from_files(spark, src_dir, file_pattern, src_file_format)
    df_transformed=transform(df)
    df_transformed=transform(df)
    df_transformed.select('repo.*').show()
    to_files(df_transformed, tgt_dir, tgt_file_format)

if __name__ == '__main__':
  main()
  
  
# aws s3 cp ~/Documents/AWS_Data_analytics/data/itv-github/landing/ghactivity s3://ghvemractivitydata/landing --recursive
