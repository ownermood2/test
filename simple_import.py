#!/usr/bin/env python3
"""
Simple direct parser - finds option lines and extracts them
"""

import os
import json
import re
import psycopg2
from docx import Document

def get_answer_key():
    """Hardcode answer key from document"""
    answers_text = """
1.d 2.a 3.a 4.a 5.a 6.a
7.c 8.c 9.a 10.a 11.c 12.b
13.b 14.a 15.d 16.b 17.a 18.d
19.b 20.a 21.c 22.c 23.c 24.c
25.c 26.a 27.a 28.c 29.c 30.c
31.a 32.d 33.b 34.a 35.d 36.a
37.d 38.a 39.d 40.d 41.a 42.a
43.a 44.a 45.a 46.c 47.a 48.a
49.a 50.a 51.b 52.d 53.b 54.a
55.c 56.b 57.b 58.b 59.b 60.b
61.a 62.a 63.c 64.c 65.a 66.a
67.a 68.a 69.c 70.c 71.d 72.d
73.d 74.a 75.a 76.a 77.d 78.a
79.d 80.d 81.a 82.a 83.a 84.a
85.a 86.c 87.d 88.a 89.a 90.a
91.b 92.b 93.b 94.b 95.b 96.b 97.b 98.a 99.a 100.b 101.a 102.a 103.c 104.c 105.c 106.c 107.c 108.c 109.c 110.a 111.d 112.d 113.d 114.d
115.d 116.d 117.d 118.d 119.a 120.a 121.c 122.a 123.a 124.d 125.a 126.a 127.a 128.a 129.a 130.d 131.b 132.a 133.b 134.a 135.b 136.b 137.b 138.a
145.a 146.c 147.a 148.c 149.c 150.c 151.a 152.d 153.d 154.a 155.d 156.d 157.d 158.a 159.a 160.d 161.a 162.d 163.a 164.a 165.a 166.a 167.a 168.d
169.a 170.c 171.a 172.b 173.b 174.a
175.a 176.b 177.b 178.a 179.a 180.a
181.c 182.c 183.c 184.c 185.a 186.b
187.c 188.c 189.a 190.a 191.d 192.a 193.d 194.a 195.a 196.d 197.d 198.a
199.d 200.a 201.d 202.a 203.a 204.a
205.a 206.a 207.b 208.a 209.c 210.a 211.a 212.b 213.b 214.a 215.b 216.a 217.a 218.b 219.b 220.a 221.c 222.c 223.c 224.c 225.a 226.a 227.c 228.c
229.c 230.a 231.d 232.a 233.c 234.b
235.a 236.d 237.b 238.a 239.a 240.b
241.d 242.b 243.a
    """
    
    answers = {}
    matches = re.findall(r'(\d+)\.([a-d])', answers_text)
    for match in matches:
        q_num = int(match[0])
        answer_letter = match[1].lower()
        answer_index = ord(answer_letter) - ord('a')
        answers[q_num] = answer_index
    
    return answers

def parse_document():
    """Parse the document line by line"""
    doc = Document('attached_assets/5_6284844636582190663_1760023077830.docx')
    
    # Get all lines
    lines = []
    for para in doc.paragraphs:
        text = para.text.strip()
        if text and 'Answers' not in text and '©' not in text and 'Reserved' not in text:
            lines.append(text)
    
    answers = get_answer_key()
    questions = []
    
    question_num = 1
    question_text = ""
    
    for line in lines:
        # Count option markers in this line
        opt_markers = len(re.findall(r'\b[a-d]\s*\)', line, re.IGNORECASE))
        
        # If line has 3 or 4 option markers, it's likely the options line
        if opt_markers >= 3:
            # Parse options
            # Try to split by letter + parenthesis
            parts = re.split(r'\s*\b([a-d])\s*\)\s*', line, flags=re.IGNORECASE)
            
            options = []
            for i in range(1, len(parts), 2):
                if i+1 < len(parts):
                    opt_text = parts[i+1].strip()
                    # Remove trailing option markers
                    opt_text = re.sub(r'\s+[a-d]\s*\).*$', '', opt_text, flags=re.IGNORECASE)
                    opt_text = opt_text.split('\t')[0].strip()
                    if opt_text:
                        options.append(opt_text)
            
            # If we got 4 options, save
            if len(options) >= 4 and question_num in answers:
                # Clean question
                q_clean = re.sub(r'…+', '', question_text).strip()
                q_clean = re.sub(r'^\d+[\.\)]\s*', '', q_clean)
                q_clean = re.sub(r'\s+', ' ', q_clean).strip()
                
                if len(q_clean) >= 10:
                    questions.append({
                        'number': question_num,
                        'question': q_clean,
                        'options': options[:4],
                        'correct_answer': answers[question_num]
                    })
                    question_num += 1
                
                # Reset
                question_text = ""
        else:
            # Accumulate question text
            question_text += " " + line
    
    return questions

def import_questions(questions):
    """Import to database"""
    database_url = os.environ.get('DATABASE_URL')
    conn = psycopg2.connect(database_url)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM questions")
    before = cursor.fetchone()[0]
    
    imported = 0
    skipped = 0
    
    for q in questions:
        try:
            options_json = json.dumps(q['options'])
            cursor.execute(
                "INSERT INTO questions (question, options, correct_answer) VALUES (%s, %s, %s)",
                (q['question'], options_json, q['correct_answer'])
            )
            imported += 1
            if imported % 50 == 0:
                print(f"  ⏳ {imported}...")
        except:
            skipped += 1
            conn.rollback()
    
    conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM questions")
    after = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()
    
    return before, after, imported, skipped

if __name__ == "__main__":
    print("=" * 70)
    print("📚 Simple Import - Direct Option Line Detection")
    print("=" * 70)
    
    print("\n🔍 Parsing document...")
    questions = parse_document()
    print(f"✅ Found {len(questions)} questions")
    
    print("\n📋 First 5:")
    for q in questions[:5]:
        print(f"\nQ{q['number']}: {q['question'][:60]}...")
        for i, opt in enumerate(q['options']):
            mark = "✓" if i == q['correct_answer'] else " "
            print(f"  {chr(65+i)}) {opt[:50]} {mark}")
    
    print("\n🗄️  Importing...")
    before, after, imported, skipped = import_questions(questions)
    print(f"\n✅ Imported: {imported}")
    print(f"⚠️  Skipped: {skipped}")
    print(f"📊 Total: {before} → {after}")
