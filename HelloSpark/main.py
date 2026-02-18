import sys
from lib.utils import *

if __name__ == "__main__":
#	if len(sys.argv) != 4:
#		print("Usage: main.py {local, qa, prod} file_path: Arguments are missing")
#		sys.exit(-1)

	spark = get_spark_session(sys.argv[1])
	file_path = sys.argv[2]
	survery_df = load_survey_df(spark, file_path)
	result_df = count_by_country(survery_df)
	result_df.show()
