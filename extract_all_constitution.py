#!/usr/bin/env python3
"""
Extract ALL Constitution questions - handles various numbering formats
"""

import os
import json
import re
import psycopg2
from docx import Document

def extract_all_questions(docx_path):
    """Extract all questions regardless of numbering format"""
    doc = Document(docx_path)
    
    # Get all text
    full_text = '\n'.join([p.text for p in doc.paragraphs])
    
    # Find all answer lines with their positions
    answer_pattern = r'(?:correct\s+)?answer\s*:\s*([a-d])'
    answer_matches = list(re.finditer(answer_pattern, full_text, re.IGNORECASE))
    
    print(f"Found {len(answer_matches)} answer lines")
    
    questions = []
    
    for i, ans_match in enumerate(answer_matches):
        answer_letter = ans_match.group(1).lower()
        answer_index = ord(answer_letter) - ord('a')
        
        # Find start of this question
        # It's either the previous answer's end, or start of document
        if i == 0:
            q_start = 0
        else:
            q_start = answer_matches[i-1].end()
        
        q_end = ans_match.start()
        
        # Extract question block
        q_block = full_text[q_start:q_end].strip()
        
        # Remove Q.No. or question number prefix
        q_block = re.sub(r'^Q\.No\.\s*\d+[\:\.]?\s*', '', q_block)
        q_block = re.sub(r'^\d+[\.\)]\s*', '', q_block)
        
        # Split into lines
        lines = [l.strip() for l in q_block.split('\n') if l.strip()]
        
        if len(lines) < 5:
            continue
        
        # First line is question
        question_text = lines[0]
        
        # Find options (lines starting with (a), (b), (c), (d))
        options = []
        
        for line in lines[1:]:
            opt_match = re.match(r'^\(([a-d])\)\s*(.+)', line, re.IGNORECASE)
            if opt_match:
                options.append(opt_match.group(2).strip())
        
        # Must have exactly 4 options
        if len(options) == 4:
            # Clean question
            question_text = re.sub(r'\s+', ' ', question_text).strip()
            
            if len(question_text) >= 10:
                questions.append({
                    'question': question_text,
                    'options': options,
                    'correct_answer': answer_index
                })
    
    return questions

def import_to_database(questions):
    """Import to database"""
    database_url = os.environ.get('DATABASE_URL')
    conn = psycopg2.connect(database_url)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM questions")
    before = cursor.fetchone()[0]
    
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
                print(f"  ⏳ {imported}...")
        except:
            skipped += 1
            conn.rollback()
    
    conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM questions")
    after = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()
    
    print(f"\n✅ Imported: {imported}")
    print(f"⚠️  Skipped: {skipped}")
    print(f"📊 Total: {before} → {after}")
    
    return imported

if __name__ == "__main__":
    print("=" * 70)
    print("📚 Extracting ALL Constitution Questions (589 answers found)")
    print("=" * 70)
    
    docx_path = 'attached_assets/INDIAN Constitution (600-MCQ) [@CLAT_Vision]_1760027225327.docx'
    
    print("\n🔍 Extracting questions from answer positions...")
    questions = extract_all_questions(docx_path)
    
    print(f"✅ Extracted {len(questions)} valid questions")
    
    # Preview
    print("\n📋 First 5:")
    for i, q in enumerate(questions[:5], 1):
        print(f"\n{i}. {q['question'][:65]}...")
        for j, opt in enumerate(q['options']):
            mark = "✓" if j == q['correct_answer'] else " "
            print(f"  {chr(65+j)}) {opt[:55]} {mark}")
    
    print("\n" + "=" * 70)
    print("🗄️  Importing...")
    print("=" * 70)
    
    imported = import_to_database(questions)
    
    if imported > 0:
        print(f"\n🎉 Success! Imported {imported} Constitution MCQs!")
