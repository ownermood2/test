#!/usr/bin/env python3
"""
Constitution MCQs - Reverse parser (from answer backwards)
"""

import os
import json
import re
import psycopg2
from docx import Document

def clean_text(text):
    """Clean text - remove 'correct' markers and extra spaces"""
    text = re.sub(r'\s*correct\s*$', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def parse_reverse(docx_path):
    """Parse from answer lines backwards"""
    doc = Document(docx_path)
    paras = [p.text.strip() for p in doc.paragraphs if p.text.strip()]
    
    questions = []
    
    # Find all answer line indices
    answer_indices = []
    for i, para in enumerate(paras):
        if re.search(r'(?:correct\s+)?answer\s*:\s*([a-d])', para, re.IGNORECASE):
            answer_indices.append(i)
    
    print(f"Found {len(answer_indices)} answer lines")
    
    for ans_idx in answer_indices:
        # Extract answer
        ans_para = paras[ans_idx]
        answer_match = re.search(r'(?:correct\s+)?answer\s*:\s*([a-d])', ans_para, re.IGNORECASE)
        if not answer_match:
            continue
        
        answer_letter = answer_match.group(1).lower()
        answer_index = ord(answer_letter) - ord('a')
        
        # Work backwards to find question start
        question_start_idx = ans_idx - 1
        
        # Find the Q.No. line before this answer
        for j in range(ans_idx - 1, max(0, ans_idx - 20), -1):
            if re.match(r'Q\.No\.\s*\d+', paras[j]):
                question_start_idx = j
                break
        
        # Extract question text
        q_para = paras[question_start_idx]
        q_text = re.sub(r'Q\.No\.\s*\d+[\:\.]?\s*', '', q_para).strip()
        
        # Collect all paragraphs between question and answer
        between_paras = []
        for j in range(question_start_idx + 1, ans_idx + 1):
            para = paras[j]
            # Clean from answer marker
            cleaned = re.sub(r'(?:correct\s+)?answer\s*:.*$', '', para, flags=re.IGNORECASE).strip()
            if cleaned:
                between_paras.append(cleaned)
        
        # Identify options
        # Strategy: Look for 4 short paragraphs (likely options) near the end
        # or merge consecutive short paragraphs that form options
        
        # Simple approach: take last 4 non-empty paragraphs as options
        options = []
        
        # Work backwards from answer, collecting options
        for j in range(len(between_paras) - 1, -1, -1):
            para = between_paras[j]
            cleaned = clean_text(para)
            
            if cleaned and len(cleaned) > 1:
                # Check if this looks like part of an option (very short, no punctuation at end)
                if len(options) > 0 and len(cleaned) < 50 and not cleaned.endswith('.'):
                    # Might be continuation of previous option
                    options[-1] = cleaned + ' ' + options[-1]
                else:
                    options.insert(0, cleaned)
            
            if len(options) >= 4:
                break
        
        # If we have more than 4, take first 4
        if len(options) > 4:
            # Extra text is part of question
            q_text += ' ' + ' '.join(options[:-4])
            options = options[-4:]
        elif len(options) < 4:
            # Not enough options, skip
            continue
        
        # Clean question
        q_text = re.sub(r'\s+', ' ', q_text).strip()
        
        if len(q_text) >= 10 and len(options) == 4:
            questions.append({
                'question': q_text,
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

if __name__ == "__main__":
    print("=" * 70)
    print("📚 Constitution MCQs - Reverse Parser")
    print("=" * 70)
    
    docx_path = 'attached_assets/INDIAN Constitution (600-MCQ) [@CLAT_Vision]_1760027225327.docx'
    
    print("\n🔍 Parsing from answer lines backwards...")
    questions = parse_reverse(docx_path)
    
    print(f"✅ Extracted {len(questions)} questions\n")
    
    # Preview
    print("📋 First 5:")
    for i, q in enumerate(questions[:5], 1):
        print(f"\n{i}. {q['question'][:65]}...")
        for j, opt in enumerate(q['options']):
            mark = "✓" if j == q['correct_answer'] else " "
            print(f"  {chr(65+j)}) {opt[:55]} {mark}")
    
    print("\n" + "=" * 70)
    print("🗄️  Importing to database...")
    print("=" * 70)
    import_to_database(questions)
