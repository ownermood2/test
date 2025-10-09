#!/usr/bin/env python3
"""
Parse text-based JSON and extract questions
"""

import json
import re
import os
import psycopg2

def parse_text_to_questions(text):
    """Parse text content to extract questions"""
    
    # Split by Q.No. to get question blocks
    blocks = re.split(r'Q\.No\.\s*\d+[\:\.]?\s*', text)
    
    questions = []
    
    for block in blocks:
        if not block.strip():
            continue
        
        # Find answer
        answer_match = re.search(r'(?:correct\s+)?answer\s*:\s*([a-d])', block, re.IGNORECASE)
        if not answer_match:
            continue
        
        answer_letter = answer_match.group(1).lower()
        answer_index = ord(answer_letter) - ord('a')
        
        # Remove answer from block
        content = block[:answer_match.start()].strip()
        
        # Split into lines
        lines = [l.strip() for l in content.split('\n') if l.strip()]
        
        if len(lines) < 5:  # Need question + 4 options
            continue
        
        # First line is question
        question_text = lines[0]
        
        # Parse options - look for (a), (b), (c), (d) patterns
        options = []
        
        for line in lines[1:]:
            # Check if line starts with option marker
            opt_match = re.match(r'^\(([a-d])\)\s*(.+)', line, re.IGNORECASE)
            if opt_match:
                options.append(opt_match.group(2).strip())
        
        # If we have 4 options, save the question
        if len(options) == 4:
            # Clean question text
            question_text = re.sub(r'\s+', ' ', question_text).strip()
            
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
    print("📚 Parsing Text JSON and Importing Questions")
    print("=" * 70)
    
    # Read the JSON file
    json_file = 'attached_assets/vertopal.com_INDIAN Constitution (600-MCQ) [@CLAT_Vision]_1760028182063.json'
    
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Get the text content (it's the first key)
    text_content = list(data.keys())[0]
    
    print(f"\n📖 Extracted text content ({len(text_content)} chars)")
    
    print("\n🔍 Parsing questions...")
    questions = parse_text_to_questions(text_content)
    
    print(f"✅ Found {len(questions)} questions")
    
    if len(questions) == 0:
        print("❌ No questions extracted!")
        exit(1)
    
    # Preview
    print("\n📋 First 5 questions:")
    for i, q in enumerate(questions[:5], 1):
        print(f"\n{i}. {q['question'][:65]}...")
        for j, opt in enumerate(q['options']):
            mark = "✓" if j == q['correct_answer'] else " "
            print(f"  {chr(65+j)}) {opt[:55]} {mark}")
    
    print("\n" + "=" * 70)
    print("🗄️  Importing to database...")
    print("=" * 70)
    
    imported = import_to_database(questions)
    
    if imported > 0:
        print(f"\n🎉 Successfully imported {imported} Constitution MCQs!")
