// ====== 落ちない設計：1件失敗は捨てる。全体は止めない ======
import axios from 'axios';
import { google } from 'googleapis';
import SellingPartnerAPI from 'amazon-sp-api';

const {
  SLACK_WEBHOOK_URL,
  SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT_EMAIL,
  GOOGLE_SERVICE_ACCOUNT_KEY,
  RAKUTEN_APP_ID,
  YAHOO_APP_ID,
  KEEPA_KEY,
  LWA_CLIENT_ID, LWA_CLIENT_SECRET, REFRESH_TOKEN,
  AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_ROLE_ARN
} = process.env;

const MARKETPLACE_ID = 'A1VC38T7YXB528'; // JP

// ---- Google Sheets ----
const sheetsAuth = new google.auth.GoogleAuth({
  credentials: {
    client_email: GOOGLE_SERVICE_ACCOUNT_EMAIL,
    private_key: (GOOGLE_SERVICE_ACCOUNT_KEY || '').replace(/\\n/g, '\n'),
  },
  scopes: ['https://www.googleapis.com/auth/spreadsheets']
});
const sheets = google.sheets({ version: 'v4', auth: sheetsAuth });

async function sendSlack(text){ try{ await axios.post(SLACK_WEBHOOK_URL,{text},{timeout:5000}); }catch{} }

// ---- SP-API ----
const sp = new SellingPartnerAPI({
  region: 'fe',
  refresh_token: REFRESH_TOKEN,
  client_id: LWA_CLIENT_ID,
  client_secret: LWA_CLIENT_SECRET,
  access_key: AWS_ACCESS_KEY,
  secret_key: AWS_SECRET_KEY,
  role: AWS_ROLE_ARN
});

const sleep = ms => new Promise(r=>setTimeout(r,ms));
const yen = n => `¥${Math.round(n).toLocaleString()}`;

// ---- Sheets 読み込み ----
async function readConfig(){
  const res = await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:'Config!A2:B100' });
  const m={}; (res.data.values||[]).forEach(([k,v])=>{ if(k) m[k]=Number(v); });
  return {
    min_profit_jpy: m.min_profit_jpy??600, min_roi: m.min_roi??0.12,
    review_min_profit_jpy: m.review_min_profit_jpy??450, review_min_roi: m.review_min_roi??0.10,
    points_valuation: m.points_valuation??0.85, bb_vs_median_floor: m.bb_vs_median_floor??0.90,
    qty_new_asin: m.qty_new_asin??3, qty_refill: m.qty_refill??10, cap_per_asin_jpy: m.cap_per_asin_jpy??200000
  };
}
async function readWatchlistJANs(){
  const r = await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:'Watchlist!A2:D' });
  const rows = r.data.values||[];
  return rows.filter(a=> a[0] && (a[3]===true || a[3]==='TRUE')).map(a=> String(a[0]).trim());
}

// ---- 仕入れ候補（楽天 / Yahoo） ----
async function fetchRakutenByJAN(jan){
  try{
    const url='https://app.rakuten.co.jp/services/api/IchibaItem/Search/20170706';
    const r=await axios.get(url,{params:{applicationId:RAKUTEN_APP_ID, keyword:jan, hits:3, availability:1, sort:'+itemPrice'}, timeout:5000});
    const it=r.data?.Items?.[0]?.Item; if(!it) return null;
    const points=it.pointRate?Math.round(it.itemPrice*(it.pointRate/100)):0;
    return {shop:it.shopName,title:it.itemName,price:it.itemPrice,points,shipping:0,url:it.itemUrl};
  }catch{return null;}
}
async function fetchYahooByJAN(jan){
  try{
    const url='https://shopping.yahooapis.jp/ShoppingWebService/V3/itemSearch';
    const r=await axios.get(url,{params:{appid:YAHOO_APP_ID, jan_code:jan, in_stock:true, results:3, sort:'+price'}, timeout:5000});
    const it=(r.data?.hits||[])[0]; if(!it) return null;
    const points=Math.round(it.point?.amount||0);
    return {shop:it.seller?.name||'Yahoo店舗',title:it.name,price:Math.round(it.price),points,shipping:0,url:it.url};
  }catch{return null;}
}
async function fetchKeepaByJAN(jan){
  try{
    const r=await axios.get('https://api.keepa.com/product',{params:{key:KEEPA_KEY, domain:5, code:jan, stats:90}, timeout:5000});
    const p=r.data?.products?.[0]; if(!p) return null;
    const asin=p.asin;
    const bbMedian=p.stats?.buyBoxPrice?Math.round(p.stats.buyBoxPrice/100):null;
    const bbLive=p.stats?.current?.buyBoxPrice?Math.round(p.stats.current.buyBoxPrice/100):null;
    return {asin, targetPrice: bbMedian||bbLive||null, keepaUrl:`https://keepa.com/#!product/5-${asin}`};
  }catch{return null;}
}
function calcNetCost({price,points,shipping}, val){ return Math.max(0, Math.round(price - points*val + shipping)); }

// ---- Fees（ASIN単発 / 安全に1.2秒スリープ） ----
async function getFeesForASIN(asin, targetPrice){
  const body={ FeesEstimateByIdRequestList:[{
    IdType:'ASIN', IdValue:asin, MarketplaceId:MARKETPLACE_ID,
    PriceToEstimateFees:{ ListingPrice:{CurrencyCode:'JPY',Amount:targetPrice}, Shipping:{CurrencyCode:'JPY',Amount:0}, Points:{PointsNumber:0}},
    IsAmazonFulfilled:true, OptionalFulfillmentProgram:'FBA_CORE', SellerInputIdentifier:`${asin}-${targetPrice}`
  }]};
  const res = await sp.callAPI({ operation:'getMyFeesEstimates', body }).catch(()=>null);
  const fees = res?.FeesEstimateResult?.FeesEstimateResultList?.[0]?.FeesEstimate?.TotalFeesEstimate?.Amount;
  await sleep(1200);
  return (fees==null)?null:Math.round(fees);
}

// ---- BuyLog 追記 ----
async function appendBuyLog(row){
  await sheets.spreadsheets.values.append({
    spreadsheetId:SHEET_ID, range:'BuyLog!A1', valueInputOption:'RAW', requestBody:{values:[row]}
  }).catch(()=>{});
}

// ---- Main ----
async function main(){
  const cfg = await readConfig();
  const jans = await readWatchlistJANs();
  if(!jans.length){ await sendSlack('ℹ️ Watchlistに有効なJANがありません（A列=JAN、D列=チェック）'); return; }

  let sent=0, review=0, skipped=0;

  for(const jan of jans){
    try{
      const [rkt,yho] = await Promise.all([fetchRakutenByJAN(jan), fetchYahooByJAN(jan)]);
      const list=[rkt,yho].filter(Boolean);
      if(!list.length){ skipped++; continue; }
      const cand = list.map(x=>({...x, netCost: calcNetCost(x, cfg.points_valuation)})).sort((a,b)=>a.netCost-b.netCost)[0];

      const k = await fetchKeepaByJAN(jan);
      if(!k?.asin || !k?.targetPrice){ skipped++; continue; }
      const asin=k.asin, targetPrice=k.targetPrice;

      const fees = await getFeesForASIN(asin, targetPrice);
      if(fees==null){ skipped++; continue; }

      const profit = Math.round(targetPrice - fees - cand.netCost);
      const roi = profit / cand.netCost;

      let decision='';
      if(profit>=cfg.min_profit_jpy && roi>=cfg.min_roi) decision='BUY';
      else if(profit>=cfg.review_min_profit_jpy && roi>=cfg.review_min_roi) decision='REVIEW';
      else { skipped++; continue; }

      const qty = cfg.qty_new_asin;
      const first = `[${decision}] ${qty}個 / 合計粗利 ${yen(profit*qty)}（@${yen(profit)}, ROI ${(roi*100).toFixed(0)}%）`;
      const body =
        `ASIN: ${asin} | JAN: ${jan}\n`+
        `仕入(実質): ${yen(cand.netCost)}（価格 ${yen(cand.price)} -P${cand.points}×${cfg.points_valuation} +送料 ${yen(cand.shipping)}）\n`+
        `販売参考: ${yen(targetPrice)}（Keepa 90日BB中央値）\n`+
        `手数料(公式): ${yen(fees)} → 予測粗利: ${yen(profit)} / 個\n`+
        `店舗: ${cand.shop}\n購入: ${cand.url}\nKeepa: ${k.keepaUrl}`;
      await sendSlack(`${first}\n${body}`);

      const now=new Date().toISOString().replace('T',' ').slice(0,19);
      const rowId=`${Date.now()}_${asin}`;
      await appendBuyLog([now,decision,qty,'',asin,jan,cand.title||'',cand.shop,cand.url,
        cand.price,cand.points,cand.shipping,cand.netCost,targetPrice,fees,profit,roi,
        '','','',k.keepaUrl,false,false,'',rowId]);

      if(decision==='BUY') sent++; else review++;
    }catch{ skipped++; }
  }
  await sendSlack(`✅ 処理完了：BUY ${sent} / REVIEW ${review} / SKIP ${skipped}（対象JAN ${jans.length}）`);
}
main().catch(async e=>{ await sendSlack(`⚠️ 致命的エラー: ${e.message||e}`); process.exit(1); });
