#!/usr/bin/env python3
"""
Import ALL history questions - Aggressive extraction
Extracts all 243 questions by treating each option set as a question
"""

import os
import json
import re
import psycopg2
from docx import Document

def extract_answer_key(text_lines):
    """Extract answer key"""
    answers = {}
    in_answers = False
    
    for line in text_lines:
        if 'Answers' in line or 'answers' in line:
            in_answers = True
            continue
        
        if in_answers and ('©' in line or 'Reserved' in line):
            break
        
        if in_answers:
            matches = re.findall(r'(\d+)\.([a-dA-D])', line)
            for match in matches:
                q_num = int(match[0])
                answer_letter = match[1].lower()
                answer_index = ord(answer_letter) - ord('a')
                answers[q_num] = answer_index
    
    return answers

def extract_all_questions(text_lines, answers):
    """
    Aggressive extraction: Find every line with 3-4 options
    Use preceding text as question
    """
    questions = []
    
    # Find answer section start
    answer_start = len(text_lines)
    for i, line in enumerate(text_lines):
        if 'Answers' in line:
            answer_start = i
            break
    
    question_lines = text_lines[:answer_start]
    
    question_num = 1
    accumulated_text = ""
    
    for i, line in enumerate(question_lines):
        line = line.strip()
        if not line:
            continue
        
        # Check if this line has 3-4 options (likely a complete option set)
        option_pattern = r'([a-d])\s*\)\s*([^a-d)]+?)(?=\s+[a-d]\s*\)|$)'
        options_found = re.findall(option_pattern, line, re.IGNORECASE)
        
        # Also try simple split
        simple_opts = re.findall(r'[a-d]\s*\)', line, re.IGNORECASE)
        
        if len(simple_opts) >= 3 or len(options_found) >= 3:
            # This line contains options - parse them
            # Split by option markers
            parts = re.split(r'\s+([a-d])\s*\)\s*', line, flags=re.IGNORECASE)
            
            parsed_options = []
            for j in range(1, len(parts), 2):
                if j+1 < len(parts):
                    opt_text = parts[j+1].strip()
                    # Clean up
                    opt_text = re.split(r'\s+[a-d]\s*\)', opt_text, maxsplit=1, flags=re.IGNORECASE)[0].strip()
                    # Remove trailing spaces and tabs
                    opt_text = opt_text.split('\t')[0].strip()
                    if opt_text and len(opt_text) > 1:
                        parsed_options.append(opt_text)
            
            # If we got 4 options, save the question
            if len(parsed_options) >= 4 and question_num <= len(answers):
                # Clean question text
                q_text = re.sub(r'…+', '', accumulated_text).strip()
                # Remove question number if present
                q_text = re.sub(r'^\d+[\.\)]\s*', '', q_text)
                q_text = re.sub(r'\s+', ' ', q_text).strip()
                
                # Must have some question text
                if len(q_text) >= 5 and question_num in answers:
                    questions.append({
                        'number': question_num,
                        'question': q_text,
                        'options': parsed_options[:4],
                        'correct_answer': answers[question_num]
                    })
                    question_num += 1
                
                # Reset accumulated text
                accumulated_text = ""
            elif len(parsed_options) >= 2:
                # Partial options, keep accumulating
                accumulated_text += " " + line
        else:
            # No options, accumulate as question text
            accumulated_text += " " + line
    
    return questions

def import_to_database(questions):
    """Import all questions to database"""
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        print("❌ DATABASE_URL not set")
        return False
    
    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM questions")
        current_count = cursor.fetchone()[0]
        print(f"\n📊 Current questions: {current_count}")
        
        imported = 0
        skipped = 0
        
        for q in questions:
            options_json = json.dumps(q['options'])
            
            try:
                cursor.execute(
                    "INSERT INTO questions (question, options, correct_answer) VALUES (%s, %s, %s)",
                    (q['question'], options_json, q['correct_answer'])
                )
                imported += 1
                
                if imported % 50 == 0:
                    print(f"  ⏳ Imported {imported}...")
                    
            except psycopg2.IntegrityError:
                skipped += 1
                conn.rollback()
                continue
        
        conn.commit()
        
        cursor.execute("SELECT COUNT(*) FROM questions")
        new_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f"\n✅ Imported {imported} new questions!")
        print(f"⚠️  Skipped {skipped} duplicates")
        print(f"📊 Total in database: {new_count}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    print("=" * 70)
    print("📚 Importing ALL History Questions")
    print("=" * 70)
    
    docx_path = 'attached_assets/5_6284844636582190663_1760023077830.docx'
    
    print("\n📖 Reading document...")
    doc = Document(docx_path)
    text_lines = [p.text.strip() for p in doc.paragraphs if p.text.strip()]
    
    print("📝 Extracting answer key...")
    answers = extract_answer_key(text_lines)
    print(f"✅ Found {len(answers)} answers")
    
    print("\n🔍 Extracting ALL questions...")
    questions = extract_all_questions(text_lines, answers)
    print(f"✅ Extracted {len(questions)} questions")
    
    # Preview
    print("\n" + "=" * 70)
    print("First 5 questions:")
    print("=" * 70)
    for q in questions[:5]:
        print(f"\nQ{q['number']}: {q['question'][:60]}...")
        for j, opt in enumerate(q['options']):
            marker = "✓" if j == q['correct_answer'] else " "
            print(f"   {chr(65+j)}) {opt[:50]}... {marker}")
    
    print("\n" + "=" * 70)
    import_to_database(questions)
