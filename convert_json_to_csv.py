import os
import json
import csv

# ตั้งค่าตำแหน่งโฟลเดอร์หลักที่มีโฟลเดอร์ปี 2018-2023
base_folder = "Data 2018-2023"  # แก้เป็นตำแหน่งจริงของโฟลเดอร์
output_csv = "data_from_json.csv"  # ชื่อไฟล์ CSV สุดท้าย

# เปิดไฟล์ CSV เพื่อเขียนข้อมูล
with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
    csv_writer = csv.writer(csvfile)

    # เขียนหัวตาราง
    header = ["Year", "File Name", "Date Delivered", "Bibrecord City", "Bibrecord Country", "Bibrecord Organization", "Affiliation City", "Affiliation Country", "Language", "Database", "Citation Title"]# "Title"
    csv_writer.writerow(header)

    # วนลูปปี 2018-2023
    for year in range(2018, 2024):
        folder_path = os.path.join(base_folder, str(year))
        
        # ตรวจสอบว่าโฟลเดอร์นั้นมีอยู่หรือไม่
        if os.path.exists(folder_path):
            # วนลูปแต่ละไฟล์ในโฟลเดอร์
            for file_name in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file_name)

                # ตรวจสอบว่าเป็นไฟล์ (ไม่มีการตรวจสอบสกุล)
                if os.path.isfile(file_path):
                    with open(file_path, 'r', encoding='utf-8') as file:
                        try:
                            # อ่านเนื้อหาของไฟล์ (สมมติว่าไฟล์มีข้อมูล JSON ในรูปแบบ text)
                            data = json.load(file)

                            # ดึงข้อมูลที่ต้องการ
                            date_delivered = data["abstracts-retrieval-response"]["item"]["ait:process-info"]["ait:date-delivered"]
                            item_affiliation = data["abstracts-retrieval-response"]["item"]["bibrecord"]["head"]["author-group"][0]["affiliation"]
                            title = data["abstracts-retrieval-response"]["item"]["bibrecord"]["head"]
                            affiliation_info = data["abstracts-retrieval-response"]['affiliation']
                            lang = data["abstracts-retrieval-response"]["language"]["@xml:lang"]
                            item_db = data["abstracts-retrieval-response"]["item"]["bibrecord"]["item-info"]


                            # สร้างแถวข้อมูล
                            row = [
                                year,
                                file_name,
                                f'{date_delivered["@year"]}-{date_delivered["@month"]}-{date_delivered["@day"]}',  
                                item_affiliation["city"],
                                item_affiliation["country"],
                                ", ".join(org["$"] for org in item_affiliation["organization"]),
                                affiliation_info["affiliation-city"],
                                affiliation_info["affiliation-country"],
                                lang,
                                ", ".join(org["$"] for org in item_db["dbcollection"]),
                                # item_head["citation_title"]
                                title['citation-title']
                            ]
                            csv_writer.writerow(row)

                        except json.JSONDecodeError:
                            print(f"Error decoding JSON in file {file_name}. File might not be JSON format.")
                        except Exception as e:
                            print(f"Error processing file {file_name}: {e}")

print(f"Data merged into {output_csv}")
