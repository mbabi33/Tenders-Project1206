
python C17.py -root /home/nerow/Project/tender1206/ML_DATA -c 45200000 -ds 01.01.2024 -de 01.01.2025 -ps 1 -pe 
source  organize_files.sh /home/nerow/Project/tender1206/ML_DATA
python parse_app_main.py -root /home/nerow/Project/tender1206/ML_DATA  -c 45200000
python parse_app_bids.py -root /home/nerow/Project/tender1206/ML_DATA -c 45200000
python parser_app_docs.py -root /home/nerow/Project/tender1206/ML_DATA -c 45200000
python download_app_doc_files.py -root /home/nerow/Project/tender1206/ML_DATA -c 45200000 -batch_size 5
