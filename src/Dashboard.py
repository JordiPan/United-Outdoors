import streamlit as st
import streamlit.components.v1 as components

st.title("嵌入 Power BI 报告")
# 将你获取的嵌入代码粘贴到这里
powerbi_embed_code = """
<iframe title="dashboard" width="600" height="373.5" src="https://app.powerbi.com/view?r=eyJrIjoiYmYwMTUzMWQtMmJmMS00NjQ3LWJkMjItODRjYmM3MTJlMzY3IiwidCI6ImEyNTg2YjliLWY4NjctNGIzYy05MzYzLTViNDM1YzVkYmM0NSIsImMiOjh9" frameborder="0" allowFullScreen="true"></iframe>
"""

components.html(powerbi_embed_code, height=600)

# 或者使用 st.markdown 直接插入 HTML 代码
st.markdown(powerbi_embed_code, unsafe_allow_html=True)