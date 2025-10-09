#!/usr/bin/env python3
"""
Import Constitution MCQs from Word document
Format: Q.No. X followed by question, options, and "correct answer : X"
"""

import os
import json
import re
import psycopg2
from docx import Document

def parse_constitution_questions(docx_path):
    """Parse questions from Constitution MCQ document"""
    doc = Document(docx_path)
    
    # Get all text
    all_text = '\n'.join([p.text.strip() for p in doc.paragraphs if p.text.strip()])
    
    # Split by question numbers
    question_blocks = re.split(r'Q\.No\.\s*\d+[\:\.]?\s*', all_text)
    
    questions = []
    
    for block in question_blocks:
        if not block.strip():
            continue
        
        # Extract correct answer first
        answer_match = re.search(r'(?:correct\s+)?answer\s*:\s*([a-d])', block, re.IGNORECASE)
        if not answer_match:
            continue
        
        answer_letter = answer_match.group(1).lower()
        answer_index = ord(answer_letter) - ord('a')
        
        # Remove answer from text
        content = re.sub(r'(?:correct\s+)?answer\s*:\s*[a-d].*$', '', block, flags=re.IGNORECASE).strip()
        
        # Split into lines
        lines = [line.strip() for line in content.split('\n') if line.strip()]
        
        if len(lines) < 5:  # Need question + 4 options minimum
            continue
        
        # First line is the question
        question_text = lines[0].strip()
        
        # Rest are options
        option_lines = lines[1:]
        
        # Parse options
        options = []
        for line in option_lines:
            # Remove option labels if present
            opt_text = re.sub(r'^\([a-d]\)\s*', '', line, flags=re.IGNORECASE).strip()
            if opt_text and len(opt_text) > 1:
                options.append(opt_text)
        
        # Take first 4 options
        if len(options) >= 4:
            # Clean question text
            question_text = re.sub(r'\s+', ' ', question_text).strip()
            
            questions.append({
                'question': question_text,
                'options': options[:4],
                'correct_answer': answer_index
            })
    
    return questions

def import_to_database(questions):
    """Import questions to PostgreSQL"""
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        print("❌ DATABASE_URL not set")
        return False
    
    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM questions")
        before = cursor.fetchone()[0]
        print(f"\n📊 Current questions in database: {before}")
        
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
                
                if imported % 100 == 0:
                    print(f"  ⏳ Imported {imported} questions...")
                    
            except psycopg2.IntegrityError:
                # Duplicate
                skipped += 1
                conn.rollback()
                continue
        
        conn.commit()
        
        cursor.execute("SELECT COUNT(*) FROM questions")
        after = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f"\n✅ Successfully imported {imported} new questions!")
        print(f"⚠️  Skipped {skipped} duplicates")
        print(f"📊 Total questions: {before} → {after}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("=" * 70)
    print("📚 Importing Constitution MCQs (600 Questions)")
    print("=" * 70)
    
    docx_path = 'attached_assets/INDIAN Constitution (600-MCQ) [@CLAT_Vision]_1760027225327.docx'
    
    print("\n📖 Parsing document...")
    questions = parse_constitution_questions(docx_path)
    
    print(f"✅ Extracted {len(questions)} questions")
    
    if len(questions) == 0:
        print("❌ No questions found!")
        exit(1)
    
    # Preview first 5
    print("\n" + "=" * 70)
    print("📋 First 5 questions:")
    print("=" * 70)
    for i, q in enumerate(questions[:5], 1):
        print(f"\n{i}. {q['question'][:70]}...")
        for j, opt in enumerate(q['options']):
            marker = "✓" if j == q['correct_answer'] else " "
            print(f"   {chr(65+j)}) {opt[:60]}... {marker}")
    
    print("\n" + "=" * 70)
    print("🗄️  Importing to database...")
    print("=" * 70)
    import_to_database(questions)
