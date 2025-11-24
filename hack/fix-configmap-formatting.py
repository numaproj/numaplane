#!/usr/bin/env python3
"""
Fix ConfigMap data formatting in install.yaml by copying properly formatted 
data from the source usde-config.yaml file.

This works around a kubectl kustomize issue where large multiline ConfigMap values
are converted from literal block scalars (|) to quoted strings with \n escapes.
"""

import re
import sys
import os


def extract_configmap_data(yaml_content, configmap_name):
    """Extract the data section lines from a ConfigMap YAML."""
    lines = yaml_content.split('\n')
    in_target_configmap = False
    in_data_section = False
    data_lines = []
    indent_level = None
    
    for i, line in enumerate(lines):
        # Look for the target ConfigMap
        if f'name: {configmap_name}' in line:
            in_target_configmap = True
            continue
        
        if in_target_configmap:
            # Found the data section
            if re.match(r'^data:\s*$', line):
                in_data_section = True
                indent_level = len(line) - len(line.lstrip())
                continue
            
            # If we're in data section, collect lines
            if in_data_section:
                # Check if we've exited the data section
                current_indent = len(line) - len(line.lstrip()) if line.strip() else indent_level + 1
                if line.strip() and current_indent <= indent_level:
                    break
                
                data_lines.append(line)
    
    return data_lines


def main():
    if len(sys.argv) != 2:
        print("Usage: fix-configmap-formatting.py <install.yaml>")
        sys.exit(1)
    
    install_yaml_path = sys.argv[1]
    
    # Derive the source usde-config.yaml path
    config_dir = os.path.dirname(install_yaml_path)
    source_path = os.path.join(config_dir, 'manager', 'usde-config.yaml')
    
    if not os.path.exists(source_path):
        print(f"Error: Source file not found: {source_path}", file=sys.stderr)
        sys.exit(1)
    
    try:
        # Read both files
        with open(install_yaml_path, 'r') as f:
            install_lines = f.readlines()
        
        with open(source_path, 'r') as f:
            source_content = f.read()
        
        # Extract the properly formatted data from source
        source_data_lines = extract_configmap_data(source_content, 'numaplane-controller-usde-config')
        
        if not source_data_lines:
            print(f"Error: Could not extract data from {source_path}", file=sys.stderr)
            sys.exit(1)
        
        # Find the USDE ConfigMap in install.yaml
        label_idx = None
        for i, line in enumerate(install_lines):
            if 'numaplane.numaproj.io/config: usde-config' in line:
                label_idx = i
                break
        
        if label_idx is None:
            print(f"Error: Could not find USDE ConfigMap in {install_yaml_path}", file=sys.stderr)
            sys.exit(1)
        
        # Scan backward to find the start of this ConfigMap
        configmap_start = None
        for i in range(label_idx - 1, max(0, label_idx - 100), -1):
            if install_lines[i].strip().startswith('apiVersion:'):
                configmap_start = i
                break
            elif install_lines[i].strip() == '---':
                configmap_start = i + 1
                break
        
        if configmap_start is None:
            print(f"Error: Could not find ConfigMap start", file=sys.stderr)
            sys.exit(1)
        
        # Find the data: line after configmap_start
        data_line_idx = None
        for i in range(configmap_start, label_idx):
            if re.match(r'^data:\s*$', install_lines[i]):
                data_line_idx = i
                break
        
        if data_line_idx is None:
            print(f"Error: Could not find data: section", file=sys.stderr)
            sys.exit(1)
        
        # Find where the data section ends (at 'kind:' line)
        data_end_idx = None
        for i in range(data_line_idx + 1, len(install_lines)):
            if install_lines[i].strip().startswith('kind:'):
                data_end_idx = i
                break
        
        if data_end_idx is None:
            print(f"Error: Could not find end of data section", file=sys.stderr)
            sys.exit(1)
        
        # Build the new content
        result_lines = []
        result_lines.extend(install_lines[:data_line_idx + 1])  # Everything up to and including 'data:'
        result_lines.extend([line + '\n' for line in source_data_lines])  # Add source data
        result_lines.extend(install_lines[data_end_idx:])  # Everything from 'kind:' onward
        
        # Write the fixed content
        original_content = ''.join(install_lines)
        fixed_content = ''.join(result_lines)
        
        if fixed_content != original_content:
            with open(install_yaml_path, 'w') as f:
                f.write(fixed_content)
            print(f"Fixed ConfigMap formatting in {install_yaml_path}")
        else:
            print(f"No changes needed in {install_yaml_path}")
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
