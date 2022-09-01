from docxtpl import DocxTemplate
from docx2pdf import convert
import pandas as pd

TEMPLATE = 'template.docx'
DF = pd.read_csv('data.csv', index_col='dc_plan_id')

def save_statement(dictonary):
    context = dictonary
    plan_number = context["plan_number"]
    doc = DocxTemplate(TEMPLATE)
    doc.render(context)
    filename = f'{plan_number}_contribution-summary-q2-2022.docx'
    filepath = r"documents/" + filename
    doc.save(filepath)

if __name__ == '__main__':
    for index, row in DF.iterrows():
        dictonary = dict(DF.loc[index])
        save_statement(dictonary)
    convert("documents/","pdfs/")