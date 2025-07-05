import pandas as pd
import base64
from io import BytesIO
import json # Import json to pass data to JavaScript

def generate_html_report(query_data, index_issues, integrity_issues, security_findings, index_suggestions, trigger_perf_results, relationship_perf_results, discovered_schema):
    """
    Generates a comprehensive HTML report of the database analysis with collapsible sections
    and an interactive D3.js query performance plot.
    """
    # Prepare query data for D3.js plot
    # Ensure numeric conversion is robust and create the column
    query_data['Numeric Execution Time (s)'] = pd.to_numeric(
        query_data['Execution Time (s)'], errors='coerce'
    ).fillna(0)

    # Ensure 'Short Label' is always created, even if query_data is empty
    if not query_data.empty:
        query_data['Short Label'] = [f'Query {i+1}' for i in range(len(query_data))]
    else:
        query_data['Short Label'] = [] # Add an empty list for consistency if DataFrame is empty

    # Convert DataFrame to JSON for D3.js
    # Only include necessary columns for the plot
    # Check if query_data is empty before attempting to select columns, to avoid issues
    if not query_data.empty:
        plot_data_json = query_data[['Query', 'Numeric Execution Time (s)', 'Optimized', 'Short Label']].to_json(orient='records')
    else:
        plot_data_json = "[]" # Empty JSON array if no data

    # No longer generating a base64 image from matplotlib.
    # The D3.js chart will be rendered directly into the HTML.

    # 3. Build the HTML content
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Dynamic Database Health Report</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
        <script src="https://d3js.org/d3.v7.min.js"></script> <!-- D3.js library -->
        <style>
            body {{
                font-family: 'Inter', sans-serif;
                background-color: #f3f4f6;
                color: #374151;
            }}
            .container {{
                max-width: 1200px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
            }}
            th, td {{
                padding: 12px 15px;
                text-align: left;
                border-bottom: 1px solid #e5e7eb;
            }}
            th {{
                background-color: #e0e7ff; /* Light blue for headers */
                font-weight: 600;
                color: #374151;
            }}
            tr:nth-child(even) {{
                background-color: #f9fafb;
            }}
            .section-title {{
                border-bottom: 2px solid #6366f1; /* Indigo border */
                padding-bottom: 8px;
                margin-bottom: 24px;
            }}
            .issue-critical {{ color: #ef4444; font-weight: 600; }} /* Red */
            .issue-warning {{ color: #f59e0b; }} /* Orange */
            .issue-good {{ color: #22c55e; }} /* Green */
            .code-block {{
                background-color: #1f2937; /* Dark gray */
                color: #f9fafb; /* Light text */
                padding: 16px;
                border-radius: 8px;
                overflow-x: auto;
                font-family: monospace;
                font-size: 0.9em;
            }}
            .sub-section {{
                background-color: #f0f4ff;
                padding: 16px;
                border-radius: 8px;
                margin-bottom: 16px;
            }}
            details > summary {{
                cursor: pointer;
                padding: 10px 0;
                font-size: 1.5rem; /* Matches h2 size */
                font-weight: 600; /* Matches h2 weight */
                color: #1f2937; /* Darker text for summary */
                list-style: none; /* Remove default marker */
                position: relative;
                border-bottom: 2px solid #6366f1;
                margin-bottom: 24px;
            }}
            details > summary::-webkit-details-marker {{
                display: none;
            }}
            details > summary::before {{
                content: '+';
                position: absolute;
                left: -20px; /* Adjust as needed */
                font-size: 1.2em;
                font-weight: bold;
                color: #6366f1;
            }}
            details[open] > summary::before {{
                content: '-';
            }}
            details[open] > summary {{
                border-bottom-color: #a5b4fc; /* Lighter border when open */
            }}
            /* D3.js specific styles */
            .bar {{
                fill: steelblue;
            }}
            .bar.unoptimized {{
                fill: #ef4444; /* Red for unoptimized */
            }}
            .bar.optimized {{
                fill: #22c55e; /* Green for optimized */
            }}
            .tooltip {{
                position: absolute;
                text-align: center;
                padding: 8px;
                background: rgba(0, 0, 0, 0.8);
                color: white;
                border-radius: 4px;
                pointer-events: none;
                opacity: 0;
                font-size: 0.8em;
                transition: opacity 0.2s;
                max-width: 300px; /* Limit tooltip width */
                word-wrap: break-word; /* Break long words */
            }}
        </style>
    </head>
    <body class="p-8">
        <div class="container mx-auto bg-white shadow-lg rounded-lg p-8">
            <h1 class="text-4xl font-bold text-center text-indigo-700 mb-8">Dynamic Database Health and Performance Report</h1>
            <p class="text-center text-gray-600 mb-12">Generated on {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

            <details> <!-- Collapsed by default -->
                <summary>0. Discovered Database Schema Overview</summary>
                <p class="text-gray-700 mb-4">This section provides an overview of the database structure discovered by the analyzer across all connected shards. This includes tables, columns, and relationships.</p>
                <div class="space-y-6">
                    """
    for shard_name, shard_info in discovered_schema['shards'].items():
        html_content += f"""
                    <div class="sub-section">
                        <h3 class="text-xl font-semibold text-indigo-700 mb-4">Shard: {shard_name}</h3>
                        <h4 class="text-lg font-semibold text-gray-800 mb-2">Tables:</h4>
                        <ul class="list-disc list-inside space-y-1 text-gray-700 mb-4">
                            """
        for table_name, table_details in shard_info['tables'].items():
            html_content += f"""<li><strong>{table_name}</strong> (PK: {', '.join(table_details['primary_key']) if table_details['primary_key'] else 'None'})"""
            html_content += f"""<details><summary class="text-base font-normal text-gray-700 my-1">Columns & Details</summary><ul class="list-circle list-inside ml-4">"""
            for col in table_details['columns']:
                html_content += f"""<li>{col['name']} (<span class="font-mono">{col['type']}</span>) {'(Nullable)' if col['nullable'] else ''}</li>"""
            html_content += f"""</ul></details></li>"""
        html_content += """
                        </ul>
                        <h4 class="text-lg font-semibold text-gray-800 mb-2">Triggers:</h4>
                        <ul class="list-disc list-inside space-y-1 text-gray-700">
                            """
        if shard_info['triggers']:
            for trigger in shard_info['triggers']:
                html_content += f"""<li><strong>{trigger['name']}</strong>: <pre class="code-block text-xs">{trigger['sql']}</pre></li>"""
        else:
            html_content += """<li>No triggers found in this shard.</li>"""
        html_content += """
                        </ul>
                    </div>
                    """
    html_content += """
                </div>
            </details>

            <details open>
                <summary>1. Query Performance Analysis</summary>
                <p class="text-gray-700 mb-6">This section provides an overview of the execution times for various *synthetic* queries generated based on your database schema. Queries marked "Unoptimized" may benefit from further investigation and indexing. Hover over each bar in the graph to see the full query identifier and details. Refer to the table below for full query details.</p>
                <div id="query-performance-chart" class="flex justify-center mb-8">
                    <!-- D3.js chart will be rendered here -->
                </div>
                <h3 class="text-xl font-semibold text-gray-800 mb-4">Query Performance Details</h3>
                <div class="overflow-x-auto overflow-y-auto max-h-64 md:max-h-96 rounded-lg shadow-md"> <!-- Added overflow and max-height classes -->
                    """
    if not query_data.empty:
        # Display Short Label and then the full Query for reference
        html_content += query_data[['Short Label', 'Query', 'Execution Time (s)', 'Optimized', 'Suggested Optimization']].to_html(index=False, classes='table-auto w-full text-sm rounded-lg')
    else:
        html_content += f"""<p class="text-gray-600">No query performance data available.</p>"""
    html_content += """
                </div>
                
                <details>
                    <summary class="text-xl font-semibold text-gray-800 mt-8 mb-4">Detailed Query Plans</summary>
                    <p class="text-gray-700 mb-4">Understanding the query plan is crucial for identifying bottlenecks. Look for "SCAN TABLE" without "USING INDEX" as a potential area for improvement.</p>
                    <div class="space-y-6">
                        """
    if not query_data.empty:
        for _, row in query_data.iterrows():
            html_content += f"""
                        <div class="bg-gray-50 p-4 rounded-lg shadow-sm border border-gray-200">
                            <p class="font-medium text-gray-900 mb-2">Query: <span class="font-normal">{row['Query']}</span></p>
                            <p class="font-medium text-gray-900 mb-2">Suggested Optimization: <span class="font-normal">{row['Suggested Optimization']}</span></p>
                            <h4 class="text-lg font-semibold text-gray-800 mb-2">Query Plan:</h4>
                            <pre class="code-block">{row['Query Plan']}</pre>
                        </div>
                        """
    else:
        html_content += f"""<p class="text-gray-600">No detailed query plans available.</p>"""
    html_content += """
                    </div>
                </details>
            </details>

            <details>
                <summary>2. Index Analysis</summary>
                <p class="text-gray-700 mb-4">This section highlights potential issues related to database indexes, including missing indexes on foreign keys or frequently queried columns, and potentially redundant indexes.</p>
                <ul class="list-disc list-inside space-y-2 text-gray-700">
                    """
    if index_issues:
        for issue in index_issues:
            html_content += f"""<li><span class="issue-warning">{issue}</span></li>"""
    else:
        html_content += """<li>No significant index issues detected.</li>"""
    html_content += """
                </ul>
            </details>

            <details>
                <summary>3. Data Integrity Checks</summary>
                <p class="text-gray-700 mb-4">Ensuring data integrity is vital for database reliability. This section reports on issues like foreign key violations and duplicate unique entries.</p>
                <ul class="list-disc list-inside space-y-2 text-gray-700">
                    """
    if integrity_issues:
        for issue in integrity_issues:
            if "Foreign Key Violation" in issue or "Duplicate Unique Constraint" in issue:
                html_content += f"""<li><span class="issue-critical">{issue}</span></li>"""
            else:
                html_content += f"""<li><span class="issue-warning">{issue}</span></li>"""
    else:
        html_content += """<li>No significant data integrity issues detected.</li>"""
    html_content += """
                </ul>
            </details>

            <details>
                <summary>4. Password and Sensitive Data Security Findings</summary>
                <p class="text-gray-700 mb-4">This analysis provides a heuristic check on the security of password fields and identifies other potentially sensitive data (e.g., emails, SSNs, credit cards). It's crucial to use strong encryption/hashing for sensitive data.</p>
                <ul class="list-disc list-inside space-y-2 text-gray-700">
                    """
    if security_findings:
        for finding in security_findings:
            if "CRITICAL" in finding:
                html_content += f"""<li><span class="issue-critical">{finding}</span></li>"""
            elif "WARNING" in finding:
                html_content += f"""<li><span class="issue-warning">{finding}</span></li>"""
            else:
                html_content += f"""<li><span class="issue-good">{finding}</span></li>"""
    else:
        html_content += """<li>No specific password or sensitive data security findings.</li>"""
    html_content += """
                </ul>
            </details>

            <details>
                <summary>5. Trigger Performance Analysis</summary>
                <p class="text-gray-700 mb-4">Triggers can introduce overhead. This section measures the performance impact of discovered 'AFTER INSERT' triggers by simulating batch inserts.</p>
                <ul class="list-disc list-inside space-y-2 text-gray-700">
                    """
    if trigger_perf_results:
        for result in trigger_perf_results:
            html_content += f"""<li><span class="text-gray-700">{result}</span></li>"""
    else:
        html_content += """<li>No trigger performance results available or no 'AFTER INSERT' triggers found.</li>"""
    html_content += """
                </ul>
            </details>

            <details>
                <summary>6. Relationship Performance Analysis (JOINs)</summary>
                <p class="text-gray-700 mb-4">This section analyzes the performance implications of foreign key relationships by testing synthetic JOIN queries. Missing indexes on join columns are a common cause of slow queries.</p>
                <ul class="list-disc list-inside space-y-2 text-gray-700">
                    """
    if relationship_perf_results:
        for result in relationship_perf_results:
            if "WARNING" in result or "MISSING" in result or "Error" in result:
                html_content += f"""<li><span class="issue-warning">{result}</span></li>"""
            else:
                html_content += f"""<li><span class="text-gray-700">{result}</span></li>"""
    else:
        html_content += """<li>No foreign key relationships found for analysis.</li>"""
    html_content += """
                </ul>
            </details>

            <details>
                <summary>7. Optimization SQL Suggestions</summary>
                <p class="text-gray-700 mb-4">Based on the index analysis, here are some SQL commands you might consider applying to optimize your database. Always test these suggestions in a development environment first.</p>
                <pre class="code-block">"""
    if index_suggestions:
        for suggestion in index_suggestions:
            html_content += f"{suggestion}\n"
    else:
        html_content += "No specific index optimization SQL suggestions at this time."
    html_content += """</pre>
            </details>

            <footer class="text-center text-gray-500 mt-12 pt-8 border-t border-gray-200">
                <p>Dynamic Database Health Analyzer</p>
            </footer>
        </div>

        <script>
            // D3.js Chart Rendering
            const queryData = """ + plot_data_json + """; // Data from Python

            if (queryData.length > 0) {
                const margin = {top: 40, right: 20, bottom: 120, left: 70}, // Increased bottom margin for x-axis labels
                      width = 1000 - margin.left - margin.right,
                      height = 500 - margin.top - margin.bottom;

                const svg = d3.select("#query-performance-chart")
                    .append("svg")
                    .attr("width", width + margin.left + margin.right)
                    .attr("height", height + margin.top + margin.bottom)
                    .append("g")
                    .attr("transform", `translate(${margin.left},${margin.top})`);

                const x = d3.scaleBand()
                    .range([0, width])
                    .padding(0.1)
                    .domain(queryData.map(d => d['Short Label'])); // Use Short Label for x-axis

                const y = d3.scaleLinear()
                    .range([height, 0])
                    .domain([0, d3.max(queryData, d => d['Numeric Execution Time (s)']) * 1.1]); // Add 10% padding

                // Add X axis (no labels, just the scale)
                svg.append("g")
                    .attr("transform", `translate(0,${height})`)
                    .call(d3.axisBottom(x).tickFormat("")); // Hide tick labels

                // Add Y axis
                svg.append("g")
                    .call(d3.axisLeft(y));

                // Y-axis label
                svg.append("text")
                    .attr("transform", "rotate(-90)")
                    .attr("y", 0 - margin.left + 10) // Adjusted position
                    .attr("x", 0 - (height / 2))
                    .attr("dy", "1em")
                    .style("text-anchor", "middle")
                    .text("Execution Time (s)");

                // X-axis label
                svg.append("text")
                    .attr("transform", `translate(${width / 2}, ${height + margin.bottom - 20})`) // Adjusted position
                    .style("text-anchor", "middle")
                    .text("Query Identifier (Hover for Details)");


                // Tooltip div
                const tooltip = d3.select("body").append("div")
                    .attr("class", "tooltip");

                // Add bars
                svg.selectAll(".bar")
                    .data(queryData)
                    .enter().append("rect")
                    .attr("class", d => `bar ${d.Optimized ? 'optimized' : 'unoptimized'}`)
                    .attr("x", d => x(d['Short Label']))
                    .attr("width", x.bandwidth())
                    .attr("y", d => y(d['Numeric Execution Time (s)']))
                    .attr("height", d => height - y(d['Numeric Execution Time (s)']))
                    .on("mouseover", function(event, d) {
                        tooltip.transition()
                            .duration(200)
                            .style("opacity", .9);
                        tooltip.html(`<strong>Query:</strong> ${d.Query}<br/><strong>Time:</strong> ${d['Numeric Execution Time (s)'].toFixed(4)}s<br/><strong>Optimized:</strong> ${d.Optimized ? 'Yes' : 'No'}`)
                            .style("left", (event.pageX + 10) + "px")
                            .style("top", (event.pageY - 28) + "px");
                    })
                    .on("mouseout", function(d) {
                        tooltip.transition()
                            .duration(500)
                            .style("opacity", 0);
                    });
            } else {
                d3.select("#query-performance-chart").html("<p class='text-red-500 font-semibold'>Query performance plot could not be generated due to empty or invalid data.</p>");
            }
        </script>
    </body>
    </html>
    """
    return html_content
