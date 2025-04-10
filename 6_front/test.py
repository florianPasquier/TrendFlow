import streamlit as st
import pandas as pd
import random
from datetime import datetime, timedelta

# --------------------------
# Mock API - Generate ads
# --------------------------
@st.cache_data
def fetch_mock_ads(n=20):
    platforms = ['Facebook', 'TikTok', 'Pinterest']
    countries = ['USA', 'France', 'Germany', 'Canada', 'UK']
    languages = ['English', 'French', 'German']
    tags = ['Trending', 'Summer hits', 'Best of the month', 'Top 100']

    ads = []
    for i in range(n):
        ad = {
            'Product': f"Product {i+1}",
            'Platform': random.choice(platforms),
            'Published': datetime.today() - timedelta(days=random.randint(0, 30)),
            'Image': f"https://via.placeholder.com/300x200?text=Ad+{i+1}",
            'Likes': random.randint(10, 10000),
            'Comments': random.randint(0, 500),
            'Shares': random.randint(0, 300),
            'Reactions': random.randint(0, 1000),
            'Country': random.choice(countries),
            'Language': random.choice(languages),
            'Tag': random.choice(tags)
        }
        ads.append(ad)
    return pd.DataFrame(ads)

# --------------------------
# UI Setup
# --------------------------
st.set_page_config(page_title="Ad Explorer", layout="wide")
st.title("📊 Ad Explorer - Mock API")

ads = fetch_mock_ads(30)

# Sidebar Filters
st.sidebar.header("Filters")
platforms = st.sidebar.multiselect("Platform", ads["Platform"].unique(), default=ads["Platform"].unique())
countries = st.sidebar.multiselect("Country", ads["Country"].unique(), default=ads["Country"].unique())
languages = st.sidebar.multiselect("Language", ads["Language"].unique(), default=ads["Language"].unique())

# Main Filters
search = st.text_input("🔍 Search by product name")
tag_filter = st.selectbox("Filter by Tag", options=["All"] + ads["Tag"].unique().tolist())
sort_by = st.selectbox("Sort by", options=["Most Likes", "Most Comments", "Newest"])

# Filter logic
filtered = ads[
    (ads["Platform"].isin(platforms)) &
    (ads["Country"].isin(countries)) &
    (ads["Language"].isin(languages))
]

if tag_filter != "All":
    filtered = filtered[filtered["Tag"] == tag_filter]

if search:
    filtered = filtered[filtered["Product"].str.contains(search, case=False)]

if sort_by == "Most Likes":
    filtered = filtered.sort_values(by="Likes", ascending=False)
elif sort_by == "Most Comments":
    filtered = filtered.sort_values(by="Comments", ascending=False)
elif sort_by == "Newest":
    filtered = filtered.sort_values(by="Published", ascending=False)

# Display cards
def display_ad(ad):
    with st.container():
        st.image(ad["Image"], use_column_width=True)
        st.subheader(ad["Product"])
        st.caption(f"{ad['Platform']} | {ad['Country']} | {ad['Language']} | {ad['Published'].date()}")
        st.markdown(f"**Likes:** {ad['Likes']} | **Comments:** {ad['Comments']} | **Shares:** {ad['Shares']} | **Reactions:** {ad['Reactions']}")
        st.markdown(f"🏷️ *{ad['Tag']}*")

# Grid layout
cols = st.columns(3)
for i, (_, ad) in enumerate(filtered.iterrows()):
    with cols[i % 3]:
        display_ad(ad)

if filtered.empty:
    st.info("No ads match your filters.")
