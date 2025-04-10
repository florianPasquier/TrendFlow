import streamlit as st
import pandas as pd
import datetime
from mock_data import get_mock_ads # Import from the local mock_data file

# --- Page Configuration ---
st.set_page_config(layout="wide", page_title="Ad Analysis Platform")
st.title("🚀 Ad Analysis Platform (Minea Simulation)")

# --- Load Data ---
@st.cache_data # Cache the data loading
def load_data():
    """Loads ad data from the mock source."""
    ads_list = get_mock_ads()
    df = pd.DataFrame(ads_list)
    # Ensure correct data types
    df['publication_date'] = pd.to_datetime(df['publication_date']).dt.date
    df['likes'] = pd.to_numeric(df['likes'])
    df['comments'] = pd.to_numeric(df['comments'])
    df['shares'] = pd.to_numeric(df['shares'])
    df['reactions'] = pd.to_numeric(df['reactions'])
    return df

df_ads = load_data()

# --- Sidebar Filters ---
st.sidebar.header("⚙️ Filters")

# 1. Platform Filter
platforms = sorted(df_ads['platform'].unique())
selected_platforms = st.sidebar.multiselect(
    "Platforms",
    options=platforms,
    default=platforms
)

# 2. Date Range Filter
min_date = df_ads['publication_date'].min()
max_date = df_ads['publication_date'].max()
selected_date_range = st.sidebar.date_input(
    "Publication Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

# 3. Country Filter
countries = sorted(df_ads['country'].unique())
selected_countries = st.sidebar.multiselect(
    "Target Countries",
    options=countries,
    default=countries
)

# 4. Language Filter
languages = sorted(df_ads['language'].unique())
selected_languages = st.sidebar.multiselect(
    "Languages",
    options=languages,
    default=languages
)

# 5. Engagement Filter (Example: Likes)
min_likes = int(df_ads['likes'].min())
max_likes = int(df_ads['likes'].max())
selected_likes_range = st.sidebar.slider(
    "Likes Range",
    min_value=min_likes,
    max_value=max_likes,
    value=(min_likes, max_likes)
)

# 6. Tags Filter
all_tags = sorted(list(set(tag for tags_list in df_ads['tags'] for tag in tags_list)))
selected_tags = st.sidebar.multiselect(
    "Tags",
    options=all_tags,
    default=[] # Default to no specific tag filter
)


# --- Main Area ---

# 1. Search Bar
search_term = st.text_input("Search by Product Name", placeholder="Enter product name...")

# 2. Sorting Options
sort_options = {
    "Popularity (Reactions)": "reactions",
    "Recency (Publication Date)": "publication_date"
}
sort_by = st.selectbox("Sort By", options=list(sort_options.keys()), index=0)
sort_ascending = st.checkbox("Sort Ascending", value=False if sort_by == "Popularity (Reactions)" else True)


# --- Filtering Logic ---
filtered_df = df_ads.copy()

# Apply Search
if search_term:
    filtered_df = filtered_df[filtered_df['product_name'].str.contains(search_term, case=False, na=False)]

# Apply Sidebar Filters
filtered_df = filtered_df[filtered_df['platform'].isin(selected_platforms)]
filtered_df = filtered_df[filtered_df['country'].isin(selected_countries)]
filtered_df = filtered_df[filtered_df['language'].isin(selected_languages)]

# Apply Date Range Filter
if len(selected_date_range) == 2:
    start_date, end_date = selected_date_range
    filtered_df = filtered_df[
        (filtered_df['publication_date'] >= start_date) &
        (filtered_df['publication_date'] <= end_date)
    ]

# Apply Likes Range Filter
min_l, max_l = selected_likes_range
filtered_df = filtered_df[
    (filtered_df['likes'] >= min_l) & (filtered_df['likes'] <= max_l)
]

# Apply Tags Filter (Match ads containing ALL selected tags)
if selected_tags:
    filtered_df = filtered_df[
        filtered_df['tags'].apply(lambda ad_tags: all(tag in ad_tags for tag in selected_tags))
    ]

# --- Sorting Logic ---
sort_column = sort_options[sort_by]
filtered_df = filtered_df.sort_values(by=sort_column, ascending=sort_ascending)

# --- Display Results ---
st.header(f"📊 Ad Results ({len(filtered_df)} found)")

if filtered_df.empty:
    st.warning("No ads match the current filter criteria.")
else:
    # Define number of columns for the grid
    num_columns = 3 # Adjust as needed for responsiveness
    ads_to_display = filtered_df.to_dict('records')

    for i in range(0, len(ads_to_display), num_columns):
        cols = st.columns(num_columns)
        batch = ads_to_display[i:i+num_columns]

        for j, ad in enumerate(batch):
            with cols[j]:
                st.subheader(ad['product_name'])
                if ad.get('image_url'):
                    st.image(ad['image_url'], use_container_width =True)

                # Display key info using columns for better layout
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Platform", ad['platform'])
                    st.metric("Country", ad['country'])
                with col2:
                    st.metric("Date", str(ad['publication_date']))
                    st.metric("Language", ad['language'])

                # Engagement metrics
                st.markdown("**Engagement:**")
                col_likes, col_comments, col_shares = st.columns(3)
                col_likes.metric("Likes 👍", f"{ad['likes']:,}")
                col_comments.metric("Comments 💬", f"{ad['comments']:,}")
                col_shares.metric("Shares 🔗", f"{ad['shares']:,}")

                # Tags
                if ad.get('tags'):
                    st.markdown("**Tags:** " + ", ".join(ad['tags']))

                st.markdown("---") # Separator between cards

# --- Footer Info (Optional) ---
st.sidebar.markdown("---")
st.sidebar.info("This is a simulation using mock data.")
