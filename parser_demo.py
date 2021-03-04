from tools import parse_index_file_by_language, extract_gzip

# parse_index_file_by_language('D:/PycharmProjects/commoncrawl/data/2021-04/cdx-00295')

file_path = extract_gzip("data/2021-04/cdx-00295.gz", "cdx-00295", folder_path="data/2021-04/")

print(file_path)