import streamlit as st
import json
import pandas as pd
import requests
from streamlit.web.server.websocket_headers import _get_websocket_headers

class SessionAuth:
    def __init__(self):
        self.user = None
        self.token = None
        self.user_tenant_ids = []
        self.user_tenants = {}
        self.logged_in = False
        self.logging_in = False
        self.tenant_id = None
        self.tenant_name = None
    def get_fd_token(self, user, password):
        headers = {
            'Content-Type': 'application/json'
            }
        body = {
            'username': user,
            'password': password
            }
        url = 'https://fluids-data-api.azurewebsites.net/login'
        response = requests.post(url, headers=headers, json=body,)
        if response.status_code == 200:
            response_dict = json.loads(response.text)
            return response_dict['data']['token']
        else:
            return None
    def get_fd_user(self, token):
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
            }
        url = 'https://fluids-data-api.azurewebsites.net/public/api/admin/me'
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            user_info = json.loads(response.text)
            return user_info
        else:
            return None
    def get_fd_tenants(self, token):
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
            }
        url = 'https://fluids-data-api.azurewebsites.net/public/api/admin/tenants'
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            j = json.loads(response.text)['data']['tenants']
            tenant_df = pd.DataFrame.from_dict(j)
            return tenant_df
        else:
            return None
    def login(self, username, password):
        login_message = 'Not logged in'
        self.user = username
        token = self.get_fd_token(username, password)
        self.token = token
        if not token:
            login_message = 'Invalid login'
        else:
            user_info = self.get_fd_user(token) 
            if user_info:
                self.user_tenant_ids = user_info['data']['user']['tenantIDs']
                tenants_df = self.get_fd_tenants(token)
                if tenants_df is None:
                    login_message = 'Not authorized'
                else:
                    user_tenants = {}
                    tenant_ids = []
                    tenant_names = []
                    for r, row in tenants_df.iterrows():
                        tenant_id = row['ID']
                        tenant_name = row['name'].strip()
                        if tenant_id in self.user_tenant_ids:
                            user_tenants[row['ID']] = row['name']
                    self.user_tenants = user_tenants
                    self.logged_in = True
                    self.logging_in = False
                    login_message = 'Success'
        return self.logged_in, login_message 
    def logout(self):
        self.user = None
        self.logged_in = False
        self.tenant_id = None
        self.token = None
        self.user_tenant_ids = None
        return True, 'Logged out'



def do_authorization_flow():

    def ux_tenant_selected():
        auth.tenant_id = st.session_state.select_tenant
        auth.tenant_name = auth.user_tenants[auth.tenant_id]
    # User management

    if 'auth' not in st.session_state:
        st.session_state['auth'] = SessionAuth()

    auth = st.session_state.auth
    if auth.user is None:
        headers = _get_websocket_headers()
        if "X-Ms-Client-Principal-Name" in headers:
            user = headers["X-Ms-Client-Principal-Name"]
            auth.user = user

    if auth.logged_in == False:
        if st.sidebar.button('Login'):
            auth.logging_in = True 

        if auth.logging_in == True:
            with st.sidebar.form('login_form'):
                username = st.text_input('User',value=auth.user)
                password = st.text_input('Password', type='password')
                c1, c2 =  st.columns([1,1])
                with c1:
                    submitted = st.form_submit_button('OK', type='primary')
                        
                    if submitted: 
                        logged_in, login_message = auth.login(username, password)
                        if logged_in == False:
                            st.sidebar.warning(login_message)
                        else:  
                            st.rerun()        
                with c2:
                    if st.form_submit_button('Cancel'):
                        auth.logging_in = False
                        st.rerun()
    else:
        st.sidebar.write(auth.user)
        if len(auth.user_tenants) == 1:
            # Extracting key and value
            auth.tenant_id = list(auth.user_tenants.keys())[0]
            auth.tenant_name = list(auth.user_tenants.values())[0]
        else:
            if auth.tenant_id is None:
                auth.tenant_id = list(auth.user_tenants.keys())[0]
                auth.tenant_name = list(auth.user_tenants.values())[0]
            st.sidebar.selectbox('select tenant', auth.user_tenants,format_func=lambda x: auth.user_tenants.get(x), key='select_tenant', on_change=ux_tenant_selected)
        if st.sidebar.button('Logout'):
            logged_out, logout_message = auth.logout()
            st.rerun()
    return auth

    
