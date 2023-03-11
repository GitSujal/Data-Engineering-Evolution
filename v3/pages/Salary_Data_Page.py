import streamlit as st
from data_preparation import default
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


st.set_page_config(page_title="Salaries For Data Professionals", page_icon="💵")
st.title("Salaries For Data Professionals")

with st.sidebar:
    st.header("About")
    st.markdown(""" ### This is a simple app to scrape data from Seek.
    * you can use it to find the top skills for data professionals 
    * you can also use it to find the salaries for those roles
    Hope this helps you in your job search! 
    """)

with st.spinner("Refreshing Data..."):
    # read the processed data
    df = pd.read_csv('data/processed/seek_scraper_processed_data.csv', na_filter=False)
    st.info("Processed {} jobs".format(len(df)), icon="ℹ️")

left_column, mid_column, right_column = st.columns(3)
with left_column:
    # job search term selector
    job_selection_list = df['searchKeywords'].unique().tolist()
    # add All to the beginning of the list
    job_selection_list.insert(0, "All")
    selection_job_search = st.selectbox('Select a search term', job_selection_list, index=0)
    # only show data for selected search term
    if selection_job_search != "All":
        sel_df = df[df['searchKeywords'] == selection_job_search]
    else:
        sel_df = df
with mid_column:
    # job location selector
    location_selection_list = sel_df['jobLocation'].unique().tolist()
    # add All to the beginning of the list
    location_selection_list.insert(0, "All")
    selection_job_location = st.selectbox('Select a location', location_selection_list, index=0)
    # only show data for selected location
    if selection_job_location != "All":
        sel_df = sel_df[sel_df['jobLocation'] == selection_job_location]

with right_column:
    # Number skill selctor for slider
    count_dict = {"Top 5": 5, "Top 10": 10, "Top 20": 20, "Top 50": 50, "All" : 100}
    selection_skill_number = st.selectbox('Select number of skills to show', list(count_dict.keys()), index=4)

l2, r2 = st.columns(2)
with l2:
    # industry selector
    industry_selection_list = sel_df['jobClassification'].unique().tolist()
    # add All to the beginning of the list
    industry_selection_list.insert(0, "All")
    selection_industry = st.selectbox('Select an industry', industry_selection_list)
    # only show data for selected industry
    if selection_industry != "All":
        sel_df = sel_df[sel_df['jobClassification'] == selection_industry]

with r2:
    # job type selector
    job_type_selection_list = sel_df['jobWorkType'].unique().tolist()
    # add All to the beginning of the list
    job_type_selection_list.insert(0, "All")
    selection_job_type = st.selectbox('Select a job type', job_type_selection_list, index=0)
    # only show data for selected job type
    if selection_job_type != "All":
        sel_df = sel_df[sel_df['jobWorkType'] == selection_job_type]


# aggregated stats
agg_df = sel_df.groupby('jobTitle').agg({'per_annum': 'mean', 
                                         'per_hour': 'mean',
                                            'per_day': 'mean',
                                             'jobTitle': 'count' }).rename(columns={'jobTitle': 'count'}).reset_index()

# make it into a dataframe
agg_df = pd.DataFrame(agg_df)

selection_type = st.radio("Select a view", ('Top by Salary', 'Top by Count'), horizontal=True)
if selection_type == 'Top by Salary':
    agg_df = agg_df.sort_values(by='per_annum', ascending=False)
    # limit the number of rows based on selection
    agg_df = agg_df.head(count_dict[selection_skill_number])
else:
    agg_df = agg_df.sort_values(by='count', ascending=False)
    # limit the number of rows based on selection
    agg_df = agg_df.head(count_dict[selection_skill_number])
        
# # produce same graph using go
fig = go.Figure(data=[go.Bar(y=agg_df['jobTitle'], x=agg_df['per_annum'], 
                             orientation='h', text=agg_df['per_annum'], 
                             textposition='outside', 

                             showlegend=False, 
                             texttemplate='%{text:.2s}', 
                             marker_color=agg_df['count'], 
                             
                             )])

# reduce graph margins
fig.update_layout(margin=dict(l=0, r=0, t=30, b=0))

fig.update_layout(yaxis=dict(autorange="reversed"))

# increase the font size and make it black and bold
fig.update_layout(font=dict(size=12, color='black'))

# increase the font size for y axis
fig.update_yaxes(title_font=dict(size=14))

# set the title
fig.update_layout(title_text=f"Salaries for {selection_skill_number} for {selection_job_search}")
# show the chart
st.plotly_chart(fig, use_container_width=True, sharing='streamlit', config={'displaylogo': False}, theme='streamlit')

# show some stats
st.info("Above chart is based on {} jobs scraped from Seek".format(len(sel_df)), icon="ℹ️")
