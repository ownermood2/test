#!/usr/bin/env python3
"""
Import Constitution MCQs - Sequential paragraph parser
"""

import os
import json
import re
import psycopg2
from docx import Document

def parse_questions_sequential(docx_path):
    """Parse by walking through paragraphs sequentially"""
    doc = Document(docx_path)
    
    paras = [p.text.strip() for p in doc.paragraphs if p.text.strip()]
    
    questions = []
    i = 0
    
    while i < len(paras):
        para = paras[i]
        
        # Check if this is a question start
        if re.match(r'Q\.No\.\s*\d+', para):
            # Extract question text (remove Q.No. prefix)
            q_text = re.sub(r'Q\.No\.\s*\d+[\:\.]?\s*', '', para).strip()
            
            # Collect following paragraphs until we find an answer
            collected_paras = []
            i += 1
            
            while i < len(paras):
                current = paras[i]
                
                # Check if this para contains answer
                answer_match = re.search(r'(?:correct\s+)?answer\s*:\s*([a-d])', current, re.IGNORECASE)
                
                if answer_match:
                    # Found answer!
                    answer_letter = answer_match.group(1).lower()
                    answer_index = ord(answer_letter) - ord('a')
                    
                    # Remove answer part from current para
                    option_text = re.sub(r'(?:correct\s+)?answer\s*:.*$', '', current, flags=re.IGNORECASE).strip()
                    if option_text:
                        collected_paras.append(option_text)
                    
                    # Now parse options
                    # Take last 4 non-empty paragraphs as options
                    option_paras = [p for p in collected_paras if p and len(p) > 1]
                    
                    if len(option_paras) >= 4:
                        # Get last 4 as options
                        options = option_paras[-4:]
                        
                        # Clean options (remove option labels if present)
                        cleaned_options = []
                        for opt in options:
                            # Remove (a), (b), (c), (d) if present
                            opt_clean = re.sub(r'^\([a-d]\)\s*', '', opt, flags=re.IGNORECASE).strip()
                            cleaned_options.append(opt_clean)
                        
                        # Anything before options is part of question
                        if len(option_paras) > 4:
                            q_text += ' ' + ' '.join(option_paras[:-4])
                        
                        # Clean question text
                        q_text = re.sub(r'\s+', ' ', q_text).strip()
                        
                        if len(q_text) >= 10:
                            questions.append({
                                'question': q_text,
                                'options': cleaned_options,
                                'correct_answer': answer_index
                            })
                    
                    i += 1
                    break
                else:
                    # No answer yet, collect this paragraph
                    collected_paras.append(current)
                    i += 1
        else:
            i += 1
    
    return questions

def import_to_database(questions):
    """Import to database"""
    database_url = os.environ.get('DATABASE_URL')
    conn = psycopg2.connect(database_url)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM questions")
    before = cursor.fetchone()[0]
    print(f"\n📊 Current: {before}")
    
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
    
    return True

if __name__ == "__main__":
    print("=" * 70)
    print("📚 Constitution MCQs - Sequential Parser")
    print("=" * 70)
    
    docx_path = 'attached_assets/INDIAN Constitution (600-MCQ) [@CLAT_Vision]_1760027225327.docx'
    
    print("\n🔍 Parsing...")
    questions = parse_questions_sequential(docx_path)
    
    print(f"✅ Found {len(questions)} questions")
    
    # Preview
    print("\n📋 First 5:")
    for i, q in enumerate(questions[:5], 1):
        print(f"\n{i}. {q['question'][:60]}...")
        for j, opt in enumerate(q['options']):
            mark = "✓" if j == q['correct_answer'] else " "
            print(f"  {chr(65+j)}) {opt[:50]} {mark}")
    
    print("\n🗄️  Importing...")
    import_to_database(questions)
