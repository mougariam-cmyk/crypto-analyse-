from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests

app = FastAPI(
    title="MARSOF AI Security Engine",
    version="1.0.0"
)

class TokenCheckRequest(BaseModel):
    contract_address: str
    chain: str = "sui"

@app.get("/")
def home():
    return {"status": "online"}

@app.post("/analyze-token")
def analyze_token(data: TokenCheckRequest):
    contract = data.contract_address.strip()
    chain = data.chain.strip().lower()
    api_url = f"https://api.gopluslabs.io/api/v1/token_security/{chain}?contract_addresses={contract}"
    
    try:
        response = requests.get(api_url, timeout=10)
        res_json = response.json()
        result_dict = res_json.get("result", {})
        
        if not result_dict or contract.lower() not in result_dict:
            raise HTTPException(status_code=404, detail="Not found")
            
        info = result_dict[contract.lower()]
        risk_score = 50 if info.get("is_honeypot", "0") == "1" else 10
        
        return {
            "status": "success",
            "contract_address": contract,
            "risk_score": risk_score,
            "report_text": f"Risk Score: {risk_score}/100"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
