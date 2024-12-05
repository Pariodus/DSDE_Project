import pandas as pd
import re

# Load your CSV file into a DataFrame
file_path = '3_organize/faculty_cu.csv'  # Replace with your file path
df = pd.read_csv(file_path)

# Define a function to clean the faculty names
def clean_faculty_name(faculty):
    # Convert to lowercase
    faculty = faculty.lower()
    
    # Remove text within parentheses and strip spaces
    faculty = re.sub(r'\s*\(.*?\)\s*', '', faculty)
    
    # Remove multiple spaces
    faculty = re.sub(r'\s+', ' ', faculty).strip()
    
    # Standardize singular form by removing trailing "s" (except "ss")
    if faculty.endswith("s") and not faculty.endswith("ss"):
        faculty = faculty[:-1]
        
    if faculty.lower().startswith('and '):
        faculty = faculty[4:]  # Remove the first 4 characters ('and ')
        
    return faculty

def group_faculty(faculty):
    if re.search(r'asean|asia|Asian', faculty, re.IGNORECASE):      #สถาบันเอเชีย
        return 'Institute of Asian Studies'
    if re.search(r'public health|social sciences', faculty, re.IGNORECASE):      #สาธารณสุข
        return 'College of Public Health Sciences'
    if re.search(r'halal', faculty, re.IGNORECASE):      #ศูนย์วิจัยฮาลาล
        return 'The Halal Science Center'
    if re.search(r'social research institute', faculty, re.IGNORECASE):      #วิจัยสังคม
        return 'Social Research Institute'
    if re.search(r'college of population studie|sport science', faculty, re.IGNORECASE):    #วิทยาลัยประชากร
        return 'College of Population Studies'
    if re.search(r'graduate|graduated|medical microbiology|cutip|southeast asian|technopreneurship|eds|nanoscience', faculty, re.IGNORECASE):        #บัณฑิตวิทยาลัย
        return 'Graduate School'
    if re.search(r'transportation institute', faculty, re.IGNORECASE):  #สถาบันการขนส่ง
        return 'Transportation Institute'
    if re.search(r'sasin|sasin graduate|sasin school', faculty, re.IGNORECASE):     #sasin
        return 'Sasin Graduate Institute of Business Administration'  
    if re.search(r'petroleum', faculty, re.IGNORECASE):  #วิทยาลัยปิโตรเลียมและปิโตรเคมี
        return 'The Petroleum and Petrochemical College' 
    if re.search(r'substance', faculty, re.IGNORECASE):  
        return 'Center of Excellence on Hazardous Substance Management' 
    if re.search(r'agricultural', faculty, re.IGNORECASE):  #สำนักวิชาทรัพยากรการเกษตร
        return 'School of Agricultural Resources'   
    if re.search(r'energy', faculty, re.IGNORECASE):  #สถาบันวิจัยพลังงาน
        return 'Energy Research Institute'  
    
    
    if re.search(r'engineer|power system|elite laboratory|sdrl|wireless', faculty, re.IGNORECASE):      #วิดวะ
        return 'Faculty of Engineering'
    if re.search(r'nurse|nursing', faculty, re.IGNORECASE):     #พยาบาล
        return 'Faculty of Nursing'
    if re.search(r'economic', faculty, re.IGNORECASE):      #เสดสาด
        return 'Faculty of Economics'
    if re.search(r'architecture', faculty, re.IGNORECASE):      #ถาปัด
        return 'Faculty of Architecture'
    if re.search(r'dentistry|radiology|oral|orthodontic', faculty, re.IGNORECASE):     #ทันตะ
        return 'Faculty of Dentistry'
    if re.search(r'sports science|sport science', faculty, re.IGNORECASE):  #วิดกี
        return 'Faculty of Sports Science'
    if re.search(r'business school|commerce|statistic|accountancy', faculty, re.IGNORECASE):      #บัญชี บริหาร
        return 'Faculty of Commerce and Accountancy'
    if re.search(r'vaccine research center|anatomy|pediatric|pathology|physiology|psychiatry|division|nephrology|orthopaedic|gastrointestinal|arrhythmia|clinical|vaccine|pharmacogenomics|diseases|disease|medicine|tropical|osteoarthritis|memorial hospital|allergy|thai red cross society|vector biology and vector borne disease research unit|human genetics|hepatology|ophthalmology|hepatitis|cancer', faculty, re.IGNORECASE):     # หมอ
        return 'Faculty of Medicine'
    if re.search(r'law', faculty, re.IGNORECASE):      #นิติ
        return 'Faculty of Law'
    if re.search(r'psychology', faculty, re.IGNORECASE):      #จิตวิทยา
        return 'Faculty of Faculty of Psychology'
    if re.search(r'education|educational invention and innovation research unit|bangkok metropolitan administration', faculty, re.IGNORECASE):      #ครุ
        return 'Faculty of Education'
    if re.search(r'age-related inflammation and degeneration research unit|allied|Allied|alied', faculty, re.IGNORECASE):      #สหเวช
        return 'Faculty of Allied Health Sciences'
    if re.search(r'drug|natural products chemistry|pharmacology|medical|parmaceutical|pharmaceutical|Phamaceutical|pharmacognosy|pharmaceutics|pharmaceuticals|pharmacy|medicinal plant', faculty, re.IGNORECASE):      #เภสัช
        return 'Faculty of Phamaceutical Science'
    if re.search(r'vet|parasitology', faculty, re.IGNORECASE):      #สัตวะ
        return 'Faculty of Veterinary Science'
    if re.search(r'applied', faculty, re.IGNORECASE):      #สินกำ
        return 'Faculty of Fine and Applied Arts'
    if re.search(r'communication', faculty, re.IGNORECASE):      #นิเทด
        return 'Faculty of Communication Arts'
    if re.search(r'bioactive resources for innovative clinical applications research unit|linguistic|english|faculty of art', faculty, re.IGNORECASE):      #อักษร
        return 'Faculty of Arts'
    if re.search(r'one health research cluster|physic|botany|printing|materials|mathematics|environmental science|biology|chemical technology|faculty of science|facultyof science|computational chemistry|chemistry|food technology|geology|biochemistry', faculty, re.IGNORECASE):      #วิดยา
        return 'Faculty of Science'
    if re.search(r'political', faculty, re.IGNORECASE):      #รัดสาด
        return 'Faculty of Political Science'
    
    
# Clean the 'Faculty' column
df['Faculty'] = df['Faculty'].apply(clean_faculty_name)
df['Faculty'] = df['Faculty'].apply(group_faculty)


# Group by 'Year' and 'Faculty', and count occurrences
grouped_df = df.groupby(['Year', 'Faculty']).size().reset_index(name='Count')

# Save the cleaned and grouped data to a new CSV file
grouped_df.to_csv("cleaned_grouped_year_faculty.csv", index=False)

# Display the result
print(grouped_df.info())


