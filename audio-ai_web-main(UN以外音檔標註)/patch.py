import sys

with open('c:\\Users\\c\\Desktop\\audio-ai_web\\app\\routers\\upload.py', 'r', encoding='utf-8') as f:
    content = f.read()

start_str = '        # 尋找對應的音檔\n        audio = AudioInfo.query.filter(AudioInfo.file_name.like(f"{base_filename}%")).first()'
end_str = '        except Exception as e:\n            errors.append(f"處理 {filename} 時發生錯誤: {str(e)}")'

start_idx = content.find(start_str)
end_idx = content.find(end_str) + len(end_str)

if start_idx == -1 or end_idx < len(end_str):
    print("Cannot find block")
    sys.exit(1)

old_block = content[start_idx:end_idx]

new_query_part = """        # 尋找對應的音檔 - 支援多個同名且未標記的音檔
        audios = AudioInfo.query.filter(AudioInfo.file_name.like(f"{base_filename}%")).all()
        if not audios:
            errors.append(f"找不到檔名為 {base_filename} 相關的音檔記錄")
            continue
            
        unlabeled_audios = []
        for a in audios:
            has_label = BBoxAnnotation.query.join(Result).filter(Result.upload_id == a.id).first() is not None
            if not has_label:
                unlabeled_audios.append(a)
                
        if not unlabeled_audios:
            errors.append(f"檔名 {base_filename} 的相關音檔皆已標記過，不進行覆蓋")
            continue
            
        try:
            df = pd.read_excel(file, header=None)
            if len(df) <= 2:
                errors.append(f"檔案 {filename} 沒有資料")
                continue
                
            for audio in unlabeled_audios:
                try:"""

processing_start = old_block.find('            params = audio.get_params()')
processing_end = old_block.find('            db.session.commit()')

if processing_start == -1 or processing_end == -1:
    print('Cannot find processing block')
    sys.exit(1)

processing_block = old_block[processing_start:processing_end]
indented_processing_block = '\n'.join('    ' + line if line.strip() else line for line in processing_block.split('\n'))
indented_processing_block = indented_processing_block.rstrip()

new_end_part = """
                db.session.commit()
                total_labels_inserted += labels_inserted
                
                except Exception as inner_e:
                    errors.append(f"處理音檔 {base_filename} (ID: {audio.id}) 時發生錯誤: {str(inner_e)}")
                    continue
            
            success_count += 1
            
        except Exception as e:
            errors.append(f"處理 {filename} 時發生錯誤: {str(e)}")"""

new_block = new_query_part + "\n" + indented_processing_block + "\n" + new_end_part

new_content = content[:start_idx] + new_block + content[end_idx:]

with open('c:\\Users\\c\\Desktop\\audio-ai_web\\app\\routers\\upload.py', 'w', encoding='utf-8') as f:
    f.write(new_content)
    
print("Success")
