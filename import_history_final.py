#!/usr/bin/env python3
"""
Import history questions - Streaming parser approach
Assigns sequential question numbers based on option patterns
"""

import os
import json
import re
import psycopg2
from docx import Document

def extract_answer_key(text_lines):
    """Extract answer key from document"""
    answers = {}
    in_answers = False
    
    for line in text_lines:
        if 'Answers' in line or 'answers' in line:
            in_answers = True
            continue
        
        if in_answers and ('©' in line or 'Reserved' in line):
            break
        
        if in_answers:
            # Find all patterns like "1.d" or "1.a"
            matches = re.findall(r'(\d+)\.([a-dA-D])', line)
            for match in matches:
                q_num = int(match[0])
                answer_letter = match[1].lower()
                answer_index = ord(answer_letter) - ord('a')
                answers[q_num] = answer_index
    
    return answers

def normalize_text(text):
    """Normalize whitespace and clean text"""
    # Remove ellipsis
    text = re.sub(r'…+', '', text)
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def is_option_line(line):
    """Check if line contains option markers a), b), c), or d)"""
    return bool(re.search(r'[a-d]\s*\)', line, re.IGNORECASE))

def extract_options_from_line(line):
    """Extract all options from a line"""
    options = []
    # Split by option patterns
    parts = re.split(r'\s+([a-d])\s*\)\s*', line, flags=re.IGNORECASE)
    
    for i in range(1, len(parts), 2):
        if i+1 < len(parts):
            opt_text = parts[i+1].strip()
            # Clean trailing option markers
            opt_text = re.split(r'\s+[a-d]\s*\)', opt_text, maxsplit=1, flags=re.IGNORECASE)[0].strip()
            if opt_text:
                options.append(opt_text)
    
    return options

def parse_questions_streaming(text_lines, answers):
    """
    Parse questions using streaming approach
    - Walk document line by line
    - Accumulate question text until options found
    - Extract exactly 4 options
    - Assign sequential numbers from answer key
    """
    questions = []
    current_question = ""
    current_options = []
    question_num = 1
    
    for line in text_lines:
        line = line.strip()
        
        if not line:
            continue
        
        # Check if we hit the answers section
        if 'Answers' in line or '©' in line or 'Reserved' in line:
            break
        
        # Check if this line contains options
        if is_option_line(line):
            # Extract options from this line
            new_options = extract_options_from_line(line)
            current_options.extend(new_options)
            
            # If we have 4+ options, save this question
            if len(current_options) >= 4:
                # Normalize question text
                q_text = normalize_text(current_question)
                
                # Only save if we have valid data
                if len(q_text) > 10 and question_num in answers:
                    questions.append({
                        'number': question_num,
                        'question': q_text,
                        'options': current_options[:4],
                        'correct_answer': answers[question_num],
                        'needs_review': len(q_text) < 20 or len(current_options) > 4
                    })
                    question_num += 1
                
                # Reset for next question
                current_question = ""
                current_options = []
        else:
            # Part of question text
            # Remove question number if present
            cleaned = re.sub(r'^\d+[\.\)]\s*', '', line)
            current_question += " " + cleaned
    
    return questions

def import_to_database(questions):
    """Import questions to PostgreSQL database"""
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        print("❌ DATABASE_URL environment variable not set")
        return False
    
    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        # Get current question count
        cursor.execute("SELECT COUNT(*) FROM questions")
        current_count = cursor.fetchone()[0]
        print(f"\n📊 Current questions in database: {current_count}")
        
        imported = 0
        skipped = 0
        flagged_for_review = 0
        
        for q in questions:
            if q['needs_review']:
                flagged_for_review += 1
            
            # Convert options to proper JSON
            options_json = json.dumps(q['options'])
            
            try:
                cursor.execute(
                    "INSERT INTO questions (question, options, correct_answer) VALUES (%s, %s, %s)",
                    (q['question'], options_json, q['correct_answer'])
                )
                imported += 1
                
                if imported % 25 == 0:
                    print(f"  ⏳ Imported {imported} questions...")
                    
            except psycopg2.IntegrityError:
                # Duplicate question, skip
                skipped += 1
                conn.rollback()
                continue
        
        conn.commit()
        
        # Get new total
        cursor.execute("SELECT COUNT(*) FROM questions")
        new_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f"\n✅ Successfully imported {imported} new questions!")
        print(f"⚠️  Skipped {skipped} questions (duplicates)")
        print(f"🔍 Flagged {flagged_for_review} questions for review (short text or extra options)")
        print(f"📊 Total questions in database: {new_count}")
        
        return True
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("=" * 70)
    print("📚 Importing History Questions - Streaming Parser")
    print("=" * 70)
    print()
    
    docx_path = 'attached_assets/5_6284844636582190663_1760023077830.docx'
    
    print("📖 Reading Word document...")
    doc = Document(docx_path)
    
    # Get all text lines
    text_lines = []
    for para in doc.paragraphs:
        text = para.text.strip()
        if text:
            text_lines.append(text)
    
    print(f"✅ Read {len(text_lines)} paragraphs")
    
    # Extract answer key
    print("\n📝 Extracting answer key...")
    answers = extract_answer_key(text_lines)
    print(f"✅ Found answers for {len(answers)} questions")
    
    # Parse questions
    print("\n🔍 Parsing questions with streaming approach...")
    questions = parse_questions_streaming(text_lines, answers)
    
    print(f"✅ Extracted {len(questions)} questions")
    print(f"📊 Expected: {len(answers)} questions")
    
    if len(questions) < len(answers) * 0.8:
        print(f"\n⚠️  WARNING: Only extracted {len(questions)}/{len(answers)} questions ({100*len(questions)//len(answers)}%)")
        print("   Document may have formatting issues. Review flagged questions.")
    
    # Show first 5 questions as preview
    print("\n" + "=" * 70)
    print("📋 Preview of first 5 questions:")
    print("=" * 70)
    for q in questions[:5]:
        review_flag = " ⚠️" if q['needs_review'] else ""
        print(f"\nQ{q['number']}: {q['question'][:70]}...{review_flag}")
        for j, opt in enumerate(q['options']):
            marker = "✓" if j == q['correct_answer'] else " "
            print(f"   {chr(65+j)}) {opt[:55]}... {marker}")
    
    print("\n" + "=" * 70)
    print("🗄️  Importing to database...")
    print("=" * 70)
    import_to_database(questions)
