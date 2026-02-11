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

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['OUTPUT_FOLDER'] = 'outputs'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max
os.makedirs('uploads', exist_ok=True)
os.makedirs('outputs', exist_ok=True)

LOGO_PATH = 'static/logo-1.png'

# Register embedded TTF fonts for Adobe compatibility
# Using DejaVuSans which is visually similar to Helvetica
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
    # Columns based on analysis: 2=Date, 3=Description, 6=Debits, 7=Credits, 8=Rejects, 9=Balance
    transactions = []
    for idx in range(15, len(df_raw)):
        row = df_raw.iloc[idx]
        date_val = row[2]
        desc_val = row[3]
        credits_val = row[7]
        
        # Filter: has Credits value AND Description contains AUTO PMT-CHASE or SURPLUS
        if pd.notna(credits_val) and credits_val != '':
            desc_str = str(desc_val) if pd.notna(desc_val) else ''
            if 'AUTO PMT-CHASE' in desc_str.upper() or 'SURPLUS' in desc_str.upper():
                # Format date
                if pd.notna(date_val):
                    if isinstance(date_val, datetime):
                        date_str = date_val.strftime('%m/%d/%Y')
                    else:
                        date_str = str(date_val)
                else:
                    date_str = ''
                
                # Format credits
                try:
                    credits_formatted = f"${float(credits_val):,.2f}"
                except:
                    credits_formatted = str(credits_val)
                
                transactions.append({
                    'Date': date_str,
                    'date_obj': date_val if isinstance(date_val, datetime) else None,
                    'Description': desc_str,
                    'Credits': credits_formatted
                })
    
    # Label catch-up transactions (multiple debits on same day)
    transactions = label_catchup_transactions(transactions)
    
    return header_info, transactions


def label_catchup_transactions(transactions):
    """When multiple transactions share the same date, label earlier ones
    with the month they cover. E.g., two debits on Feb 10 means the first
    is 'for' January, the second is for February (no label)."""
    from collections import defaultdict
    from dateutil.relativedelta import relativedelta
    
    # Group transactions by date string
    date_groups = defaultdict(list)
    for i, t in enumerate(transactions):
        date_groups[t['Date']].append(i)
    
    for date_str, indices in date_groups.items():
        if len(indices) <= 1:
            continue  # Only one transaction on this date, skip
        
        # Multiple transactions on same date — label catch-ups
        # Last one is current month (no label), earlier ones go back in time
        num = len(indices)
        # Get the date object from the first transaction in this group
        date_obj = transactions[indices[0]].get('date_obj')
        
        for pos, idx in enumerate(indices):
            months_back = num - 1 - pos  # first = furthest back, last = 0
            if months_back > 0 and date_obj:
                for_date = date_obj - relativedelta(months=months_back)
                for_label = for_date.strftime('%B %Y')  # e.g., "January 2026"
                transactions[idx]['Date'] = f"{date_str}\n(For: {for_label})"
    
    return transactions


def generate_vod_pdf(header_info, transactions, output_path):
    """Generate VOD PDF with header info and filtered transactions."""
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
    label_style = ParagraphStyle(
        'Label',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#666666'),
        fontName='DejaVuSans'
    )
    value_style = ParagraphStyle(
        'Value',
        parent=styles['Normal'],
        fontSize=10,
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
    
    # Header info table (2 columns) - only show 5 fields
    header_fields = [
        ('Name', 'Account #'),
        ('Client ID', 'Status'),
        ('Date Opened', None)
    ]
    
    # Map display names to actual data keys
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
            header_data.append([
                f"{left_field}:",
                left_val,
                f"{right_field}:",
                right_val
            ])
        else:
            header_data.append([
                f"{left_field}:",
                left_val,
                '',
                ''
            ])
    
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
    if transactions:
        trans_data = [['Date', 'Description', 'Credits']]
        for t in transactions:
            trans_data.append([t['Date'], t['Description'], t['Credits']])
        
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
    
    # Footer with generation date
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
    # Name is typically "LASTNAME,FIRSTNAME" format
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
                output_filename = generate_filename(header_info)
                output_path = os.path.join(app.config['OUTPUT_FOLDER'], f"{session_id}_{output_filename}")
                
                generate_vod_pdf(header_info, transactions, output_path)
                
                results.append({
                    'original': filename,
                    'output': output_filename,
                    'path': f"{session_id}_{output_filename}",
                    'name': header_info.get('Name', 'Unknown'),
                    'transaction_count': len(transactions),
                    'success': True
                })
            except Exception as e:
                results.append({
                    'original': filename,
                    'error': str(e),
                    'success': False
                })
            finally:
                if os.path.exists(upload_path):
                    os.remove(upload_path)
    
    return jsonify({
        'session_id': session_id,
        'results': results,
        'total': len(results),
        'successful': sum(1 for r in results if r['success'])
    })


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
                # Use the clean filename (without session prefix) in the zip
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
