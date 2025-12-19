diwnload_app_docs.py

просмотр среды если все на месте 

выбираем tender_db_id которые удовлетворяют условию

"SELECT DISTINCT tender_db_id FROM tnd_app_doc_files WHERE download_status = 'pending' ORDER BY tender_db_id Limit"  =====> tender_ids_to_process
если записи есть то выбираем все
"SELECT id, file_url, tender_code, tender_db_id, section_id, file_name FROM tnd_app_doc_files WHERE download_status = 'pending' AND tender_db_id IN (?,?,?) ORDER BY tender_db_id, id" ====>files_to_download


но в принципе можно делать сразу 

SELECT id, file_url, tender_code, tender_db_id, section_id, file_name 
FROM tnd_app_doc_files 
WHERE download_status = 'pending'
 AND tender_db_id IN (
 SELECT DISTINCT tender_db_id FROM tnd_app_doc_files WHERE download_status = 'pending' LIMIT 1
 )
ORDER BY tender_db_id, id
