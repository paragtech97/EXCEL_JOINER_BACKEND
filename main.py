from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import tempfile
from fuzzywuzzy import fuzz
import itertools
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://excel-joiner-frontend.onrender.com"],  # Your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def compute_value_overlap(col1, col2):
    set1 = set(col1.dropna().astype(str))
    set2 = set(col2.dropna().astype(str))
    if not set1 or not set2:
        return 0
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return len(intersection) / len(union)

def is_column_unique(series):
    return series.nunique(dropna=True) / series.count() >= 0.7

def score_column_pair(col1_name, col2_name, df1, df2):
    score = 0
    fuzz_score = fuzz.ratio(col1_name.lower(), col2_name.lower())
    score += fuzz_score * 0.4
    dtype_match = str(df1[col1_name].dtype) == str(df2[col2_name].dtype)
    score += (25 if dtype_match else 0)
    overlap_score = compute_value_overlap(df1[col1_name], df2[col2_name])
    score += overlap_score * 35
    uniq1 = is_column_unique(df1[col1_name])
    uniq2 = is_column_unique(df2[col2_name])
    if not (uniq1 and uniq2):
        score -= 20
    return score

def suggest_join_columns(df1, df2):
    column_pairs = list(itertools.product(df1.columns, df2.columns))
    scored = [(col1, col2, round(score_column_pair(col1, col2, df1, df2), 2)) for col1, col2 in column_pairs]
    return sorted(scored, key=lambda x: x[2], reverse=True)

@app.post("/join")
async def join_excels(file1: UploadFile = File(...), file2: UploadFile = File(...)):
    try:
        df1 = pd.read_excel(file1.file)
        df2 = pd.read_excel(file2.file)

        suggestions = suggest_join_columns(df1, df2)
        print("🔍 Join suggestions:", suggestions)

        if not suggestions:
            raise HTTPException(status_code=400, detail="No joinable columns found.")

        best_col1, best_col2, _ = suggestions[0]

        joined_df = pd.merge(df1, df2, left_on=best_col1, right_on=best_col2, how='inner')

        temp_dir = tempfile.mkdtemp()
        output_path = os.path.join(temp_dir, "joined_output.xlsx")
        joined_df.to_excel(output_path, index=False)

        return FileResponse(output_path, filename="joined_output.xlsx", media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    except Exception as e:
        print(f"❌ Error during join: {e}")
        raise HTTPException(status_code=500, detail="Failed to process files.")