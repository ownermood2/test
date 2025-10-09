#!/usr/bin/env python3
"""
Import history questions from Word document - Version 2
Better parser that handles complex formatting
"""

import os
import json
import re
import psycopg2
from docx import Document

def extract_answer_key(text_lines):
    """Extract answer key from text lines"""
    answers = {}
    in_answers = False
    
    for line in text_lines:
        if 'Answers' in line or 'answers' in line:
            in_answers = True
            continue
        
        if in_answers and ('©' in line or 'Reserved' in line):
            break
        
        if in_answers:
            # Find all patterns like "1.d" or "1.a" etc.
            matches = re.findall(r'(\d+)\.([a-dA-D])', line)
            for match in matches:
                q_num = int(match[0])
                answer_letter = match[1].lower()
                # Convert letter to index (a=0, b=1, c=2, d=3)
                answer_index = ord(answer_letter) - ord('a')
                answers[q_num] = answer_index
    
    return answers

def clean_text(text):
    """Clean question text"""
    # Remove ellipsis
    text = re.sub(r'…+', '', text)
    # Remove excessive whitespace
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def extract_questions_v2(docx_path):
    """Extract questions with better parsing logic"""
    doc = Document(docx_path)
    
    # Get all text lines
    text_lines = []
    for para in doc.paragraphs:
        text = para.text.strip()
        if text:
            text_lines.append(text)
    
    # Extract answer key first
    print("📝 Extracting answer key...")
    answers = extract_answer_key(text_lines)
    print(f"✅ Found answers for {len(answers)} questions")
    
    # Find where answer section starts
    answer_start = len(text_lines)
    for i, line in enumerate(text_lines):
        if 'Answers' in line or 'answers' in line:
            answer_start = i
            break
    
    # Process only question section
    question_lines = text_lines[:answer_start]
    
    questions = []
    i = 0
    
    while i < len(question_lines):
        line = question_lines[i]
        
        # Look for question number pattern
        q_match = re.match(r'^(\d+)[\.\)]\s*(.+)', line)
        
        if q_match:
            q_num = int(q_match.group(1))
            q_text = q_match.group(2).strip()
            
            # Collect full question text (may span multiple lines)
            i += 1
            options = []
            
            # Look ahead to collect question text and options
            while i < len(question_lines):
                next_line = question_lines[i]
                
                # Stop if we hit next numbered question
                if re.match(r'^\d+[\.\)]', next_line):
                    break
                
                # Check if this line contains options (a), b), c), d))
                if re.search(r'[a-d]\)', next_line, re.IGNORECASE):
                    # Parse options from this line
                    # Split by option patterns
                    parts = re.split(r'\s+([a-d])\s*\)\s*', next_line, flags=re.IGNORECASE)
                    
                    for j in range(1, len(parts), 2):
                        if j+1 < len(parts):
                            opt_text = parts[j+1].strip()
                            # Clean up (remove trailing option markers)
                            opt_text = re.split(r'\s+[a-d]\s*\)', opt_text, flags=re.IGNORECASE)[0].strip()
                            if opt_text:
                                options.append(opt_text)
                    
                    i += 1
                    
                    # If we have 4 options, we're done with this question
                    if len(options) >= 4:
                        break
                else:
                    # Part of question text
                    q_text += ' ' + next_line.strip()
                    i += 1
            
            # Clean question text
            q_text = clean_text(q_text)
            
            # Only add if we have valid data
            if q_num in answers and len(options) >= 4 and len(q_text) > 10:
                questions.append({
                    'number': q_num,
                    'question': q_text,
                    'options': options[:4],
                    'correct_answer': answers[q_num]
                })
        else:
            i += 1
    
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
        
        for q in questions:
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
        print(f"📊 Total questions in database: {new_count}")
        
        return True
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("=" * 70)
    print("📚 Importing History Questions - Version 2")
    print("=" * 70)
    print()
    
    docx_path = 'attached_assets/5_6284844636582190663_1760023077830.docx'
    
    print("📖 Parsing Word document...")
    questions = extract_questions_v2(docx_path)
    
    print(f"\n✅ Extracted {len(questions)} questions from document")
    
    if len(questions) == 0:
        print("❌ No questions found. Check document format.")
        exit(1)
    
    # Show first 5 questions as preview
    print("\n" + "=" * 70)
    print("📋 Preview of first 5 questions:")
    print("=" * 70)
    for i, q in enumerate(questions[:5], 1):
        print(f"\n{q['number']}. {q['question'][:80]}...")
        for j, opt in enumerate(q['options']):
            marker = "✓" if j == q['correct_answer'] else " "
            print(f"   {chr(65+j)}) {opt[:60]}... {marker}")
    
    print("\n" + "=" * 70)
    print("🗄️  Importing to database...")
    print("=" * 70)
    import_to_database(questions)
