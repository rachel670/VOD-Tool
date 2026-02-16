from flask import Flask, request, jsonify, send_file, send_from_directory
from werkzeug.utils import secure_filename
import pandas as pd
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import zipfile
import io
import uuid
from collections import defaultdict, Counter
from dateutil.relativedelta import relativedelta

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['OUTPUT_FOLDER'] = 'outputs'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max
os.makedirs('uploads', exist_ok=True)
os.makedirs('outputs', exist_ok=True)

LOGO_PATH = 'static/logo-1.png'

# Register embedded TTF fonts for Adobe compatibility
FONT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fonts')
pdfmetrics.registerFont(TTFont('DejaVuSans', os.path.join(FONT_DIR, 'DejaVuSans.ttf')))
pdfmetrics.registerFont(TTFont('DejaVuSans-Bold', os.path.join(FONT_DIR, 'DejaVuSans-Bold.ttf')))
pdfmetrics.registerFont(TTFont('DejaVuSans-Oblique', os.path.join(FONT_DIR, 'DejaVuSans-Oblique.ttf')))
pdfmetrics.registerFont(TTFont('DejaVuSans-BoldOblique', os.path.join(FONT_DIR, 'DejaVuSans-BoldOblique.ttf')))
pdfmetrics.registerFontFamily(
    'DejaVuSans',
    normal='DejaVuSans',
    bold='DejaVuSans-Bold',
    italic='DejaVuSans-Oblique',
    boldItalic='DejaVuSans-BoldOblique'
)


def parse_rfms_excel(filepath):
    """Parse RFMS Excel file and extract header info + transactions."""
    df_raw = pd.read_excel(filepath, header=None)
    
    header_info = {}
    
    # Row 7 (index 7): Name, Account Type, Account #
    row7 = df_raw.iloc[7]
    for val in row7.dropna():
        val = str(val).strip()
        if val.startswith('Name:'):
            header_info['Name'] = val.replace('Name:', '').strip()
        elif val.startswith('Account Type:'):
            header_info['Account Type'] = val.replace('Account Type:', '').strip()
        elif val.startswith('Account #:'):
            header_info['Account #'] = val.replace('Account #:', '').strip()
    
    # Row 8 (index 8): Allowance, Direct Deposit #
    row8 = df_raw.iloc[8]
    for val in row8.dropna():
        val = str(val).strip()
        if val.startswith('Allowance:'):
            header_info['Allowance'] = val.replace('Allowance:', '').strip()
        elif val.startswith('Direct Deposit #:'):
            header_info['Direct Deposit #'] = val.replace('Direct Deposit #:', '').strip()
    
    # Row 9 (index 9): Date Opened, Current Balance
    row9 = df_raw.iloc[9]
    for val in row9.dropna():
        val = str(val).strip()
        if val.startswith('Date Opened:'):
            header_info['Date Opened'] = val.replace('Date Opened:', '').strip()
        elif val.startswith('Current Balance:'):
            header_info['Current Balance'] = val.replace('Current Balance:', '').strip()
    
    # Row 10 (index 10): Res ID, Status Reason
    row10 = df_raw.iloc[10]
    for val in row10.dropna():
        val = str(val).strip()
        if val.startswith('Res ID:'):
            header_info['Res ID'] = val.replace('Res ID:', '').strip()
        elif val.startswith('Status Reason:'):
            header_info['Status Reason'] = val.replace('Status Reason:', '').strip()
    
    # Row 11 (index 11): Status, Restraints
    row11 = df_raw.iloc[11]
    for val in row11.dropna():
        val = str(val).strip()
        if val.startswith('Status:'):
            header_info['Status'] = val.replace('Status:', '').strip()
        elif val.startswith('Restraints:'):
            header_info['Restraints'] = val.replace('Restraints:', '').strip()
    
    # Row 12 (index 12): Interest
    row12 = df_raw.iloc[12]
    for val in row12.dropna():
        val = str(val).strip()
        if val.startswith('Interest'):
            header_info['Interest'] = val.replace('Interest :', '').replace('Interest:', '').strip()
    
    # Transaction table starts at row 14 (index 14 is header)
    transactions = []
    for idx in range(15, len(df_raw)):
        row = df_raw.iloc[idx]
        date_val = row[2]
        desc_val = row[3]
        credits_val = row[7]
        
        # Filter: has Credits value AND Description contains AUTO PMT or SURPLUS
        if pd.notna(credits_val) and credits_val != '':
            desc_str = str(desc_val) if pd.notna(desc_val) else ''
            if 'AUTO PMT' in desc_str.upper() or 'SURPLUS' in desc_str.upper():
                # Format date
                if pd.notna(date_val):
                    if isinstance(date_val, datetime):
                        date_str = date_val.strftime('%m/%d/%Y')
                        date_obj = date_val
                    else:
                        date_str = str(date_val)
                        date_obj = None
                else:
                    date_str = ''
                    date_obj = None
                
                # Parse credits as float
                try:
                    credits_float = float(credits_val)
                except:
                    credits_float = 0.0
                
                credits_formatted = f"${credits_float:,.2f}"
                
                transactions.append({
                    'Date': date_str,
                    'date_obj': date_obj,
                    'Description': desc_str,
                    'Credits': credits_formatted,
                    'credits_float': credits_float
                })
    
    return header_info, transactions


def analyze_transactions(transactions):
    """Analyze transactions and determine which need review.
    
    Logic:
    - 1 deposit per calendar month matching usual surplus -> auto_ok (no label)
    - Multiple identical deposits in same month matching usual surplus -> auto_labeled (backwards)
    - Any deposit not matching usual surplus in a multi-deposit month -> needs_input
    
    Returns:
        needs_review: bool
        analyzed: list of transaction dicts with 'status', 'label', 'show_enrollment_option'
    """
    if not transactions:
        return False, []
    
    # Step 1: Find the usual surplus (most common credit amount)
    amounts = [t['credits_float'] for t in transactions]
    amount_counts = Counter(amounts)
    usual_surplus = amount_counts.most_common(1)[0][0] if amount_counts else 0
    
    # Step 2: Group transactions by calendar month (YYYY-MM)
    month_groups = defaultdict(list)
    for i, t in enumerate(transactions):
        if t['date_obj']:
            month_key = t['date_obj'].strftime('%Y-%m')
            month_groups[month_key].append(i)
        else:
            month_groups['unknown'].append(i)
    
    # Step 3: Initialize analyzed list
    analyzed = []
    needs_review = False
    
    for t in transactions:
        entry = dict(t)
        entry['status'] = 'auto_ok'
        entry['label'] = None
        entry['show_enrollment_option'] = False
        analyzed.append(entry)
    
    # Step 4: Analyze each month group
    for month_key, indices in month_groups.items():
        if month_key == 'unknown':
            for idx in indices:
                analyzed[idx]['status'] = 'needs_input'
                amt = analyzed[idx]['credits_float']
                analyzed[idx]['show_enrollment_option'] = amt <= 300 and amt == int(amt)
                needs_review = True
            continue
        
        if len(indices) == 1:
            # Single deposit in a month - no label needed
            analyzed[indices[0]]['status'] = 'auto_ok'
            continue
        
        # Multiple deposits in the same month
        amounts_this_month = [transactions[i]['credits_float'] for i in indices]
        all_same = len(set(amounts_this_month)) == 1
        
        # Sort indices by date within the month
        sorted_indices = sorted(indices, key=lambda i: transactions[i]['date_obj'] or datetime.min)
        
        # Parse the month for label generation
        year = int(month_key[:4])
        month = int(month_key[5:])
        current_month_date = datetime(year, month, 1)
        
        if all_same and amounts_this_month[0] == usual_surplus:
            # All identical and match usual surplus -> auto-label backwards
            num = len(sorted_indices)
            for pos, idx in enumerate(sorted_indices):
                months_back = num - 1 - pos
                if months_back > 0:
                    for_date = current_month_date - relativedelta(months=months_back)
                    analyzed[idx]['label'] = f"For: {for_date.strftime('%b %Y')}"
                    analyzed[idx]['status'] = 'auto_labeled'
                    needs_review = True
                else:
                    # Last one = current month, no label needed
                    analyzed[idx]['status'] = 'auto_ok'
        else:
            # Different amounts in same month
            surplus_indices = [i for i in sorted_indices if transactions[i]['credits_float'] == usual_surplus]
            non_surplus_indices = [i for i in sorted_indices if transactions[i]['credits_float'] != usual_surplus]
            
            # Handle surplus-matching deposits
            if len(surplus_indices) == 1:
                # Single surplus deposit this month - it's for this month
                analyzed[surplus_indices[0]]['status'] = 'auto_ok'
            elif len(surplus_indices) > 1:
                # Multiple surplus deposits - backwards label
                num_surplus = len(surplus_indices)
                for pos, idx in enumerate(surplus_indices):
                    months_back = num_surplus - 1 - pos
                    if months_back > 0:
                        for_date = current_month_date - relativedelta(months=months_back)
                        analyzed[idx]['label'] = f"For: {for_date.strftime('%b %Y')}"
                        analyzed[idx]['status'] = 'auto_labeled'
                        needs_review = True
                    else:
                        analyzed[idx]['status'] = 'auto_ok'
            
            # Flag non-surplus amounts for review
            for idx in non_surplus_indices:
                analyzed[idx]['status'] = 'needs_input'
                amt = analyzed[idx]['credits_float']
                analyzed[idx]['show_enrollment_option'] = amt <= 300 and amt == int(amt)
                needs_review = True
    
    return needs_review, analyzed


def generate_vod_pdf(header_info, transactions, output_path):
    """Generate VOD PDF with header info and labeled transactions."""
    doc = SimpleDocTemplate(
        output_path,
        pagesize=letter,
        rightMargin=0.75*inch,
        leftMargin=0.75*inch,
        topMargin=0.5*inch,
        bottomMargin=0.5*inch
    )
    
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=18,
        alignment=TA_CENTER,
        spaceAfter=20,
        textColor=colors.HexColor('#4a6741'),
        fontName='DejaVuSans-Bold'
    )
    
    story = []
    
    # Logo
    if os.path.exists(LOGO_PATH):
        img = Image(LOGO_PATH, width=2.5*inch, height=1*inch)
        img.hAlign = 'CENTER'
        story.append(img)
        story.append(Spacer(1, 0.2*inch))
    
    # Title
    story.append(Paragraph("Verification of Deposit", title_style))
    story.append(Spacer(1, 0.3*inch))
    
    # Header info table
    header_fields = [
        ('Name', 'Account #'),
        ('Client ID', 'Status'),
        ('Date Opened', None)
    ]
    
    field_mapping = {
        'Name': 'Name',
        'Client ID': 'Res ID',
        'Account #': 'Account #',
        'Status': 'Status',
        'Date Opened': 'Date Opened'
    }
    
    header_data = []
    for left_field, right_field in header_fields:
        left_key = field_mapping.get(left_field, left_field)
        left_val = header_info.get(left_key, 'N/A')
        if right_field:
            right_key = field_mapping.get(right_field, right_field)
            right_val = header_info.get(right_key, 'N/A')
            header_data.append([f"{left_field}:", left_val, f"{right_field}:", right_val])
        else:
            header_data.append([f"{left_field}:", left_val, '', ''])
    
    header_table = Table(header_data, colWidths=[1.3*inch, 2*inch, 1.5*inch, 2*inch])
    header_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'DejaVuSans-Bold'),
        ('FONTNAME', (2, 0), (2, -1), 'DejaVuSans-Bold'),
        ('FONTNAME', (1, 0), (1, -1), 'DejaVuSans'),
        ('FONTNAME', (3, 0), (3, -1), 'DejaVuSans'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('TEXTCOLOR', (0, 0), (0, -1), colors.HexColor('#555555')),
        ('TEXTCOLOR', (2, 0), (2, -1), colors.HexColor('#555555')),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ]))
    story.append(header_table)
    story.append(Spacer(1, 0.4*inch))
    
    # Transactions table
    visible_transactions = [t for t in transactions if t.get('label') != 'ENROLLMENT_FEE']
    
    # Style for date cells with labels
    date_style = ParagraphStyle(
        'DateCell',
        fontName='DejaVuSans',
        fontSize=9,
        leading=11,
    )
    label_note_style = ParagraphStyle(
        'LabelNote',
        fontName='DejaVuSans-Oblique',
        fontSize=8,
        leading=10,
        textColor=colors.HexColor('#555555'),
    )
    desc_style = ParagraphStyle(
        'DescCell',
        fontName='DejaVuSans',
        fontSize=9,
        leading=11,
    )
    credits_style = ParagraphStyle(
        'CreditsCell',
        fontName='DejaVuSans',
        fontSize=9,
        leading=11,
        alignment=2,  # RIGHT
    )
    
    if visible_transactions:
        trans_data = [['Date', 'Description', 'Credits']]
        for t in visible_transactions:
            label = t.get('label', '')
            if label and label != 'ENROLLMENT_FEE':
                date_cell = Paragraph(
                    f"{t['Date']}<br/><i><font size='8' color='#555555'>({label})</font></i>",
                    date_style
                )
            else:
                date_cell = Paragraph(t['Date'], date_style)
            
            desc_cell = Paragraph(t['Description'], desc_style)
            credits_cell = Paragraph(t['Credits'], credits_style)
            
            trans_data.append([date_cell, desc_cell, credits_cell])
        
        trans_table = Table(trans_data, colWidths=[1.8*inch, 3.2*inch, 1.5*inch])
        trans_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4a6741')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'DejaVuSans-Bold'),
            ('FONTNAME', (0, 1), (-1, -1), 'DejaVuSans'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('ALIGN', (2, 1), (2, -1), 'RIGHT'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
            ('TOPPADDING', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 6),
            ('TOPPADDING', (0, 1), (-1, -1), 6),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#cccccc')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f5f5f5')]),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        story.append(trans_table)
    else:
        no_trans_style = ParagraphStyle('NoTrans', fontName='DejaVuSans-Oblique', fontSize=10)
        story.append(Paragraph("No qualifying transactions found.", no_trans_style))
    
    # Footer
    story.append(Spacer(1, 0.5*inch))
    eastern = ZoneInfo('America/New_York')
    gen_date = datetime.now(eastern).strftime('%m/%d/%Y %I:%M %p')
    footer_style = ParagraphStyle('Footer', fontSize=8, textColor=colors.gray, fontName='DejaVuSans-Oblique')
    story.append(Paragraph(f"Generated: {gen_date}", footer_style))
    
    doc.build(story)
    return output_path


def generate_filename(header_info):
    """Generate filename: LastName, FirstName - VOD MM-DD-YYYY.pdf"""
    name = header_info.get('Name', 'Unknown')
    parts = name.split(',')
    if len(parts) >= 2:
        last_name = parts[0].strip().title()
        first_name = parts[1].strip().title()
        formatted_name = f"{last_name}, {first_name}"
    else:
        formatted_name = name.title()
    
    date_str = datetime.now().strftime('%m-%d-%Y')
    return f"{formatted_name} - VOD {date_str}.pdf"


@app.route('/')
def index():
    return send_from_directory('.', 'index.html')


@app.route('/upload', methods=['POST'])
def upload_files():
    """Upload and analyze files. Returns analysis for review if needed."""
    if 'files' not in request.files:
        return jsonify({'error': 'No files provided'}), 400
    
    files = request.files.getlist('files')
    if not files or files[0].filename == '':
        return jsonify({'error': 'No files selected'}), 400
    
    results = []
    session_id = str(uuid.uuid4())[:8]
    
    for file in files:
        if file and file.filename.endswith('.xlsx'):
            filename = secure_filename(file.filename)
            upload_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{session_id}_{filename}")
            file.save(upload_path)
            
            try:
                header_info, transactions = parse_rfms_excel(upload_path)
                needs_review, analyzed = analyze_transactions(transactions)
                
                if needs_review:
                    # Keep file saved, return analysis for review screen
                    results.append({
                        'original': filename,
                        'name': header_info.get('Name', 'Unknown'),
                        'needs_review': True,
                        'saved_file': f"{session_id}_{filename}",
                        'transactions': [
                            {
                                'index': i,
                                'date': t['Date'],
                                'description': t['Description'],
                                'credits': t['Credits'],
                                'credits_float': t['credits_float'],
                                'status': t['status'],
                                'label': t.get('label'),
                                'show_enrollment_option': t.get('show_enrollment_option', False)
                            }
                            for i, t in enumerate(analyzed)
                        ],
                        'success': True
                    })
                else:
                    # No review needed - generate PDF immediately
                    output_filename = generate_filename(header_info)
                    output_path = os.path.join(app.config['OUTPUT_FOLDER'], f"{session_id}_{output_filename}")
                    generate_vod_pdf(header_info, analyzed, output_path)
                    
                    if os.path.exists(upload_path):
                        os.remove(upload_path)
                    
                    results.append({
                        'original': filename,
                        'output': output_filename,
                        'path': f"{session_id}_{output_filename}",
                        'name': header_info.get('Name', 'Unknown'),
                        'transaction_count': len(transactions),
                        'needs_review': False,
                        'success': True
                    })
            except Exception as e:
                results.append({
                    'original': filename,
                    'error': str(e),
                    'success': False,
                    'needs_review': False
                })
                if os.path.exists(upload_path):
                    os.remove(upload_path)
    
    return jsonify({
        'session_id': session_id,
        'results': results,
        'total': len(results),
        'successful': sum(1 for r in results if r['success'])
    })


@app.route('/generate', methods=['POST'])
def generate_with_labels():
    """Generate PDF after user provides labels for flagged transactions."""
    data = request.json
    saved_file = data.get('saved_file')
    labels = data.get('labels', {})  # {index_str: label_string or 'ENROLLMENT_FEE'}
    
    if not saved_file:
        return jsonify({'error': 'No file specified'}), 400
    
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], saved_file)
    if not os.path.exists(filepath):
        return jsonify({'error': 'File not found. Please re-upload.'}), 404
    
    try:
        header_info, transactions = parse_rfms_excel(filepath)
        _, analyzed = analyze_transactions(transactions)
        
        # Apply user-provided labels
        for idx_str, label in labels.items():
            idx = int(idx_str)
            if 0 <= idx < len(analyzed):
                analyzed[idx]['label'] = label
        
        output_filename = generate_filename(header_info)
        session_id = saved_file.split('_')[0]
        output_path = os.path.join(app.config['OUTPUT_FOLDER'], f"{session_id}_{output_filename}")
        
        generate_vod_pdf(header_info, analyzed, output_path)
        
        if os.path.exists(filepath):
            os.remove(filepath)
        
        return jsonify({
            'success': True,
            'output': output_filename,
            'path': f"{session_id}_{output_filename}",
            'name': header_info.get('Name', 'Unknown'),
            'transaction_count': len([t for t in analyzed if t.get('label') != 'ENROLLMENT_FEE'])
        })
    except Exception as e:
        return jsonify({'error': str(e), 'success': False}), 500


@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory(app.config['OUTPUT_FOLDER'], filename, as_attachment=True)


@app.route('/download-all', methods=['POST'])
def download_all():
    data = request.json
    filenames = data.get('files', [])
    
    if not filenames:
        return jsonify({'error': 'No files specified'}), 400
    
    memory_file = io.BytesIO()
    with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        for filename in filenames:
            filepath = os.path.join(app.config['OUTPUT_FOLDER'], filename)
            if os.path.exists(filepath):
                clean_name = '_'.join(filename.split('_')[1:]) if '_' in filename else filename
                zf.write(filepath, clean_name)
    
    memory_file.seek(0)
    date_str = datetime.now().strftime('%m-%d-%Y')
    return send_file(
        memory_file,
        mimetype='application/zip',
        as_attachment=True,
        download_name=f'VOD_Batch_{date_str}.zip'
    )


if __name__ == '__main__':
    os.makedirs('uploads', exist_ok=True)
    os.makedirs('outputs', exist_ok=True)
    app.run(debug=True, host='0.0.0.0', port=5000)
