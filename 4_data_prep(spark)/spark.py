from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import re

# สร้าง Spark session โดยตั้งค่าการใช้ระบบไฟล์ภายในเครื่อง
spark = SparkSession.builder \
    .appName("FacultyGrouping") \
    .config("spark.hadoop.fs.local", "true") \
    .getOrCreate()

# โหลดไฟล์ CSV เข้าสู่ Spark DataFrame
file_path = 'C:/Users/HP/Desktop/New folder/DSDE_Project/3_organize/faculty_cu.csv'  # แทนที่ด้วยพาธของไฟล์ของคุณ
df = spark.read.csv(file_path, header=True, inferSchema=True)

# ฟังก์ชันทำความสะอาดข้อมูลชื่อคณะ
def clean_faculty_name(faculty):
    faculty = faculty.lower()
    faculty = re.sub(r'\s*\(.*?\)\s*', '', faculty)
    faculty = re.sub(r'\s+', ' ', faculty).strip()
    if faculty.endswith("s") and not faculty.endswith("ss"):
        faculty = faculty[:-1]
    if faculty.lower().startswith('and '):
        faculty = faculty[4:]  # ลบคำว่า 'and '
    return faculty

# ฟังก์ชันจัดกลุ่มคณะ
def group_faculty(faculty):
    if re.search(r'asean|asia|Asian', faculty, re.IGNORECASE):      # สถาบันเอเชีย
        return 'Institute of Asian Studies'
    if re.search(r'public health|social sciences', faculty, re.IGNORECASE):      # สาธารณสุข
        return 'College of Public Health Sciences'
    if re.search(r'halal', faculty, re.IGNORECASE):      # ศูนย์วิจัยฮาลาล
        return 'The Halal Science Center'
    if re.search(r'social research institute', faculty, re.IGNORECASE):      # วิจัยสังคม
        return 'Social Research Institute'
    if re.search(r'college of population studie|sport science', faculty, re.IGNORECASE):    # วิทยาลัยประชากร
        return 'College of Population Studies'
    if re.search(r'graduate|graduated|medical microbiology|cutip|southeast asian|technopreneurship|eds|nanoscience', faculty, re.IGNORECASE):        # บัณฑิตวิทยาลัย
        return 'Graduate School'
    if re.search(r'transportation institute', faculty, re.IGNORECASE):  # สถาบันการขนส่ง
        return 'Transportation Institute'
    if re.search(r'sasin|sasin graduate|sasin school', faculty, re.IGNORECASE):     # sasin
        return 'Sasin Graduate Institute of Business Administration'
    if re.search(r'petroleum', faculty, re.IGNORECASE):  # วิทยาลัยปิโตรเลียมและปิโตรเคมี
        return 'The Petroleum and Petrochemical College'
    if re.search(r'substance', faculty, re.IGNORECASE):  
        return 'Center of Excellence on Hazardous Substance Management'
    if re.search(r'agricultural', faculty, re.IGNORECASE):  # สำนักวิชาทรัพยากรการเกษตร
        return 'School of Agricultural Resources'
    if re.search(r'energy', faculty, re.IGNORECASE):  # สถาบันวิจัยพลังงาน
        return 'Energy Research Institute'

    if re.search(r'engineer|power system|elite laboratory|sdrl|wireless', faculty, re.IGNORECASE):      # วิศวะ
        return 'Faculty of Engineering'
    if re.search(r'nurse|nursing', faculty, re.IGNORECASE):     # พยาบาล
        return 'Faculty of Nursing'
    if re.search(r'economic', faculty, re.IGNORECASE):      # เศรษฐศาสตร์
        return 'Faculty of Economics'
    if re.search(r'architecture', faculty, re.IGNORECASE):      # สถาปัตย์
        return 'Faculty of Architecture'
    if re.search(r'dentistry|radiology|oral|orthodontic', faculty, re.IGNORECASE):     # ทันตแพทย์
        return 'Faculty of Dentistry'
    if re.search(r'sports science|sport science', faculty, re.IGNORECASE):  # วิทยาศาสตร์กีฬา
        return 'Faculty of Sports Science'
    if re.search(r'business school|commerce|statistic|accountancy', faculty, re.IGNORECASE):      # บัญชี/บริหาร
        return 'Faculty of Commerce and Accountancy'
    if re.search(r'vaccine research center|anatomy|pediatric|pathology|physiology|psychiatry|division|nephrology|orthopaedic|gastrointestinal|arrhythmia|clinical|vaccine|pharmacogenomics|diseases|disease|medicine|tropical|osteoarthritis|memorial hospital|allergy|thai red cross society|vector biology and vector borne disease research unit|human genetics|hepatology|ophthalmology|hepatitis|cancer', faculty, re.IGNORECASE):     # แพทยศาสตร์
        return 'Faculty of Medicine'
    if re.search(r'law', faculty, re.IGNORECASE):      # นิติศาสตร์
        return 'Faculty of Law'
    if re.search(r'psychology', faculty, re.IGNORECASE):      # จิตวิทยา
        return 'Faculty of Psychology'
    if re.search(r'education|educational invention and innovation research unit|bangkok metropolitan administration', faculty, re.IGNORECASE):      # ครุศาสตร์
        return 'Faculty of Education'
    if re.search(r'age-related inflammation and degeneration research unit|allied|Allied|alied', faculty, re.IGNORECASE):      # สหเวช
        return 'Faculty of Allied Health Sciences'
    if re.search(r'drug|natural products chemistry|pharmacology|medical|pharmaceutical|pharmacognosy|pharmaceutics|pharmaceuticals|pharmacy|medicinal plant', faculty, re.IGNORECASE):      # เภสัชศาสตร์
        return 'Faculty of Pharmaceutical Science'
    if re.search(r'vet|parasitology', faculty, re.IGNORECASE):      # สัตวศาสตร์
        return 'Faculty of Veterinary Science'
    if re.search(r'applied', faculty, re.IGNORECASE):      # สินกำ
        return 'Faculty of Fine and Applied Arts'
    if re.search(r'communication', faculty, re.IGNORECASE):      # นิเทศศาสตร์
        return 'Faculty of Communication Arts'
    if re.search(r'bioactive resources for innovative clinical applications research unit|linguistic|english|faculty of art', faculty, re.IGNORECASE):      # อักษรศาสตร์
        return 'Faculty of Arts'
    if re.search(r'one health research cluster|physic|botany|printing|materials|mathematics|environmental science|biology|chemical technology|faculty of science|facultyof science|computational chemistry|chemistry|food technology|geology|biochemistry', faculty, re.IGNORECASE):      # วิทยาศาสตร์
        return 'Faculty of Science'
    if re.search(r'political', faculty, re.IGNORECASE):      # รัฐศาสตร์
        return 'Faculty of Political Science'

# ลงทะเบียน UDFs
clean_faculty_udf = udf(clean_faculty_name, StringType())
group_faculty_udf = udf(group_faculty, StringType())

# ใช้ UDFs ในการทำความสะอาดและจัดกลุ่มข้อมูลในคอลัมน์ 'Faculty'
df_cleaned = df.withColumn('Faculty', clean_faculty_udf(col('Faculty')))
df_cleaned = df_cleaned.withColumn('Faculty', group_faculty_udf(col('Faculty')))

# จัดกลุ่มตาม 'Year' และ 'Faculty' และนับจำนวน
grouped_df = df_cleaned.groupBy('Year', 'Faculty').count().withColumnRenamed('count', 'Count')

# กำหนดพาธของเอาท์พุต (ตรวจสอบให้แน่ใจว่าเป็นโฟลเดอร์ไม่ใช่ไฟล์)
output_path = 'C:/Users/HP/Desktop/New folder/DSDE_Project/4_data_prep'  # แทนที่ด้วยพาธโฟลเดอร์ของคุณ

# เขียนข้อมูลที่ทำความสะอาดและจัดกลุ่มไปยังไฟล์ CSV ใหม่
# grouped_df.write.mode('overwrite').option('header', 'true').csv(output_path)
grouped_df.repartition(1).write.option("header", "true").csv(output_path, mode = 'append')

# แสดงผลลัพธ์
grouped_df.show()