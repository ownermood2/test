#!/usr/bin/env python3
"""
Final Constitution import - handles unlabeled options
"""

import os
import json
import re
import psycopg2
from docx import Document

def extract_questions_final(docx_path):
    """Extract questions with unlabeled options"""
    doc = Document(docx_path)
    full_text = '\n'.join([p.text for p in doc.paragraphs])
    
    # Find all answer positions
    answer_matches = list(re.finditer(r'(?:correct\s+)?answer\s*:\s*([a-d])', full_text, re.IGNORECASE))
    
    print(f"Found {len(answer_matches)} answers")
    
    questions = []
    
    for i, ans_match in enumerate(answer_matches):
        answer_letter = ans_match.group(1).lower()
        answer_index = ord(answer_letter) - ord('a')
        
        # Get text from previous answer (or start) to this answer
        if i == 0:
            q_start = 0
        else:
            q_start = answer_matches[i-1].end()
        
        q_block = full_text[q_start:ans_match.start()].strip()
        
        # Remove Q.No. prefix and question numbers
        q_block = re.sub(r'^Q\.No\.\s*\d+[\:\.]?\s*', '', q_block)
        q_block = re.sub(r'^\d+[\.\)]\s*', '', q_block)
        q_block = re.sub(r'^correct\s*$', '', q_block, flags=re.IGNORECASE)
        
        # Split into lines
        lines = [l.strip() for l in q_block.split('\n') if l.strip() and l.strip().lower() != 'correct']
        
        # Need at least 5 lines (question + 4 options)
        if len(lines) < 5:
            continue
        
        # First line is question
        question_text = lines[0]
        
        # Next 4 lines are options (unlabeled)
        if len(lines) >= 5:
            options = lines[1:5]
        else:
            # If less than 5 lines total, skip
            continue
        
        # Clean question text
        question_text = re.sub(r'\s+', ' ', question_text).strip()
        
        # Clean options
        clean_options = []
        for opt in options:
            opt_clean = re.sub(r'\s+', ' ', opt).strip()
            if len(opt_clean) > 0:
                clean_options.append(opt_clean)
        
        # Must have exactly 4 options and valid question
        if len(clean_options) == 4 and len(question_text) >= 10:
            questions.append({
                'question': question_text,
                'options': clean_options,
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

if __name__ == "__main__":
    print("=" * 70)
    print("📚 Final Constitution Import - Unlabeled Options")
    print("=" * 70)
    
    docx_path = 'attached_assets/INDIAN Constitution (600-MCQ) [@CLAT_Vision]_1760027225327.docx'
    
    print("\n🔍 Extracting questions...")
    questions = extract_questions_final(docx_path)
    
    print(f"✅ Extracted {len(questions)} questions")
    
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
    
    import_to_database(questions)
