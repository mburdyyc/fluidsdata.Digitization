# Introduction 
The Digitization utility is a Streamlit application that maps customer PVT report content to FluidsData standard data structures (derived from the ProdML standard). 
PVT reports in PDF format are first put through an OCR process at Nanonets.com to extract tables and text (currently outside the scope of this application).
This application allows users to review the extracted data, identify tables and fields of interest, select the corresponding FluidsData tables, and map columns between the report tables and FluidsData tables.
The application records all of the unique mappings that are done and uses them to automatically idenitify and map similar tables.
The application validates that the extracted and mapped data is correct from schema perspective, i.e. all mandatory fields are entered and field data types are correct.
The application allows users to map unit of unit of measure and component names used in their reports to standard FluidsData terminology (derived from the ProdML standard).
Name mapping is automatically applied to subsequent reports.
The final validated data is stored for visualization and other purposes.

# Getting Started
TODO: Guide users through getting your code up and running on their own system. In this section you can talk about:
1.	Installation process
2.	Software dependencies
3.	Latest releases
4.	API references

# Build and Test
TODO: Describe and show how to build your code and run the tests. 

# Contribute
TODO: Explain how other users and developers can contribute to make your code better. 

