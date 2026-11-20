"""
Management Command: Test Intersection between Proposal GeoJSON and Layer

Usage:
    # Test with default sample polygon
    python manage.py test_intersection --layer "Boranga_FloraOCCBoundaryActive"

    # Test with GeoJSON file
    python manage.py test_intersection --layer "Boranga_FloraOCCBoundaryActive" --geojson /path/to/proposal.geojson

    # Test with coordinates
    python manage.py test_intersection --layer "CPT_THREATENED_FAUNA" --coords "115.0,-32.0,115.5,-31.5"

    # Save results
    python manage.py test_intersection --layer "Wheatbelt_Wetlands" --output result.geojson

    # Test with specific intersection type
    python manage.py test_intersection --layer "320" --how "Inside"

    # Verbose output
    python manage.py test_intersection --layer "DIE_INFESTED" -v 2
"""

import json
import time
import sys
from pathlib import Path

import geopandas as gpd
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings

from sqs.components.gisquery.models import Layer
from sqs.utils.loader_utils import DbLayerProvider


class Command(BaseCommand):
    help = 'Test geometric intersection between proposal GeoJSON and layer'

    def add_arguments(self, parser):
        parser.add_argument(
            '--layer',
            type=str,
            required=True,
            help='Layer name (e.g., "Boranga_FloraOCCBoundaryActive")'
        )

        parser.add_argument(
            '--geojson',
            type=str,
            help='Path to GeoJSON file. If not provided, uses default sample polygon.'
        )

        parser.add_argument(
            '--coords',
            type=str,
            help='Polygon coordinates as "lon1,lat1,lon2,lat2" (creates bounding box). '
                 'Example: "115.0,-32.0,115.5,-31.5"'
        )

        parser.add_argument(
            '--output',
            type=str,
            help='Save results to file (GeoJSON, CSV, or Excel). '
                 'Example: "result.geojson" or "result.csv"'
        )

        parser.add_argument(
            '--how',
            type=str,
            choices=['Overlapping', 'Inside', 'Outside'],
            default='Overlapping',
            help='Type of spatial intersection (default: Overlapping)'
        )

        parser.add_argument(
            '--limit',
            type=int,
            default=10,
            help='Limit number of results to display (default: 10)'
        )

        parser.add_argument(
            '--show-columns',
            type=int,
            default=5,
            help='Number of columns to show (default: 5)'
        )

        parser.add_argument(
            '--format',
            type=str,
            choices=['table', 'json', 'csv'],
            default='table',
            help='Output format (default: table)'
        )

        parser.add_argument(
            '--all-columns',
            action='store_true',
            help='Show all columns (instead of limited columns)'
        )

        parser.add_argument(
            '--select-columns',
            type=str,
            help='Comma-separated list of columns to show. Example: "id,name,geometry"'
        )

        parser.add_argument(
            '--timing',
            action='store_true',
            help='Show timing information'
        )

    def handle(self, *args, **options):
        try:
            # Validate arguments
            layer_name = options['layer']
            coords = options.get('coords')
            geojson_file = options.get('geojson')

            verbosity = int(options.get('verbosity', 1))

            if verbosity >= 1:
                self.stdout.write(self.style.SUCCESS('\n' + '='*80))
                self.stdout.write(self.style.SUCCESS('INTERSECTION TEST'))
                self.stdout.write(self.style.SUCCESS('='*80 + '\n'))

            # =====================================================================
            # Step 1: Create Proposal GeoJSON
            # =====================================================================
            if verbosity >= 1:
                self.stdout.write(self.style.WARNING('[Step 1] Creating/Loading Proposal GeoJSON...'))

            start_time = time.time() if options['timing'] else None

            if geojson_file:
                # Load from file
                geojson_path = Path(geojson_file)
                if not geojson_path.exists():
                    raise CommandError(f'GeoJSON file not found: {geojson_file}')

                with open(geojson_path, 'r') as f:
                    geojson = json.load(f)

                if verbosity >= 1:
                    self.stdout.write(f'  ✓ Loaded from: {geojson_file}')

            elif coords:
                # Create from coordinates
                try:
                    lon1, lat1, lon2, lat2 = map(float, coords.split(','))
                except ValueError:
                    raise CommandError('Invalid coordinates format. Use: "lon1,lat1,lon2,lat2"')

                geojson = {
                    "type": "FeatureCollection",
                    "features": [{
                        "type": "Feature",
                        "properties": {"name": "sample_proposal"},
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [[
                                [lon1, lat1],
                                [lon2, lat1],
                                [lon2, lat2],
                                [lon1, lat2],
                                [lon1, lat1]
                            ]]
                        }
                    }]
                }

                if verbosity >= 1:
                    self.stdout.write(f'  ✓ Created polygon: [{lon1}-{lon2}, {lat1}-{lat2}]')

            else:
                # Use default sample
                geojson = {
                    "type": "FeatureCollection",
                    "features": [{
                        "type": "Feature",
                        "properties": {"name": "sample_proposal"},
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [[
                                [115.0, -32.0],
                                [115.5, -32.0],
                                [115.5, -31.5],
                                [115.0, -31.5],
                                [115.0, -32.0]
                            ]]
                        }
                    }]
                }

                if verbosity >= 1:
                    self.stdout.write(f'  ✓ Using default sample polygon')

            # Load into GeoPandas
            try:
                shapefile_gdf = gpd.read_file(json.dumps(geojson))
                if verbosity >= 1:
                    self.stdout.write(f'  ✓ Loaded into GeoPandas')
                    self.stdout.write(f'    - CRS: {shapefile_gdf.crs}')
                    self.stdout.write(f'    - Bounds: {shapefile_gdf.total_bounds}')
            except Exception as e:
                raise CommandError(f'Error loading GeoJSON: {str(e)}')

            if options['timing']:
                elapsed = time.time() - start_time
                if verbosity >= 1:
                    self.stdout.write(f'  ⏱ Time: {elapsed:.2f}s')

            # =====================================================================
            # Step 2: Get Layer from Database
            # =====================================================================
            if verbosity >= 1:
                self.stdout.write(self.style.WARNING('[Step 2] Loading Layer from Database...'))

            start_time = time.time() if options['timing'] else None

            try:
                layer_obj = Layer.objects.get(name=layer_name)
                layer_url = layer_obj.url
                if verbosity >= 1:
                    self.stdout.write(f'  ✓ Layer: {layer_name}')
                    self.stdout.write(f'    - URL: {layer_url[:70]}...')
            except Layer.DoesNotExist:
                available = Layer.objects.all().values_list('name', flat=True)[:10]
                raise CommandError(
                    f'Layer "{layer_name}" not found.\n'
                    f'Available layers: {", ".join(available)}'
                )

            # Load layer data
            try:
                layer_provider = DbLayerProvider(layer_name, url=layer_url)
                layer_info, layer_gdf = layer_provider.get_layer()

                if verbosity >= 1:
                    self.stdout.write(f'  ✓ Layer loaded successfully')
                    self.stdout.write(f'    - CRS: {layer_info["layer_crs"]}')
                    self.stdout.write(f'    - Features: {len(layer_gdf):,}')
                    self.stdout.write(f'    - Bounds: {layer_gdf.total_bounds}')
                    self.stdout.write(f'    - Columns: {len(layer_gdf.columns)}')

            except Exception as e:
                raise CommandError(f'Error loading layer: {str(e)}')

            if options['timing']:
                elapsed = time.time() - start_time
                if verbosity >= 1:
                    self.stdout.write(f'  ⏱ Time: {elapsed:.2f}s')

            # =====================================================================
            # Step 3: Align CRS
            # =====================================================================
            if verbosity >= 1:
                self.stdout.write(self.style.WARNING('[Step 3] Aligning Coordinate Reference Systems...'))

            start_time = time.time() if options['timing'] else None

            if layer_info['layer_crs'].lower() != shapefile_gdf.crs.srs.lower():
                if verbosity >= 1:
                    self.stdout.write(
                        f'  → Converting from {shapefile_gdf.crs} to {layer_info["layer_crs"]}'
                    )
                shapefile_gdf.to_crs(layer_info['layer_crs'], inplace=True)
                if verbosity >= 1:
                    self.stdout.write(f'  ✓ CRS aligned')
            else:
                if verbosity >= 1:
                    self.stdout.write(f'  ✓ Already aligned ({shapefile_gdf.crs})')

            if options['timing']:
                elapsed = time.time() - start_time
                if verbosity >= 1:
                    self.stdout.write(f'  ⏱ Time: {elapsed:.2f}s')

            # =====================================================================
            # Step 4: PERFORM INTERSECTION
            # =====================================================================
            if verbosity >= 1:
                self.stdout.write(self.style.SUCCESS('[Step 4] ⭐ PERFORMING INTERSECTION ⭐'))
                self.stdout.write(f'  Code: layer_gdf.overlay(proposal_gdf, how="intersection")')

            start_time = time.time() if options['timing'] else None

            try:
                overlay_gdf = layer_gdf.overlay(
                    shapefile_gdf[['geometry']],
                    how='intersection',
                    keep_geom_type=False
                )
                if verbosity >= 1:
                    self.stdout.write(f'  ✓ Intersection complete!')
            except Exception as e:
                raise CommandError(f'Error during intersection: {str(e)}')

            if options['timing']:
                elapsed = time.time() - start_time
                if verbosity >= 1:
                    self.stdout.write(f'  ⏱ Time: {elapsed:.2f}s')

            # =====================================================================
            # Step 5: Handle Different Intersection Types
            # =====================================================================
            if verbosity >= 1 and options['how'] != 'Overlapping':
                self.stdout.write(self.style.WARNING(f'[Step 5] Applying "{options["how"]}" Filter...'))

            if options['how'] == 'Inside':
                # Features completely inside the proposal
                diff_gdf = layer_gdf.overlay(
                    shapefile_gdf[['geometry']],
                    how='difference',
                    keep_geom_type=False
                )
                overlay_gdf = layer_gdf[~layer_gdf.index.isin(diff_gdf.index)]

            elif options['how'] == 'Outside':
                # Features completely outside the proposal
                overlay_gdf = layer_gdf[~layer_gdf.index.isin(overlay_gdf.index)]

            # =====================================================================
            # Step 6: Display Results
            # =====================================================================
            if verbosity >= 1:
                self.stdout.write(self.style.SUCCESS('\n[Step 6] Results'))

            self.stdout.write(self.style.SUCCESS(f'\n✓ Features intersected: {len(overlay_gdf):,}'))
            self.stdout.write(f'  Result shape: {overlay_gdf.shape}')

            if len(overlay_gdf) > 0:
                columns = list(overlay_gdf.columns)
                show_cols = options['show_columns']
                if len(columns) > show_cols:
                    self.stdout.write(
                        f'  Columns (showing {show_cols} of {len(columns)}): {columns[:show_cols]} ...'
                    )
                else:
                    self.stdout.write(f'  Columns: {columns}')

                # Display results based on format
                if options['format'] == 'table':
                    self._display_table(overlay_gdf, options['limit'], options['show_columns'],
                                      all_columns=options.get('all_columns', False),
                                      select_columns=options.get('select_columns'))
                elif options['format'] == 'json':
                    self._display_json(overlay_gdf, options['limit'],
                                     all_columns=options.get('all_columns', False),
                                     select_columns=options.get('select_columns'))
                elif options['format'] == 'csv':
                    self._display_csv(overlay_gdf, options['limit'],
                                    all_columns=options.get('all_columns', False),
                                    select_columns=options.get('select_columns'))

            else:
                self.stdout.write(self.style.WARNING(
                    '\n⚠ No features found in intersection'
                ))
                self.stdout.write('  → Proposal might not overlap with layer')
                self.stdout.write(f'  → Proposal bounds: {shapefile_gdf.total_bounds}')
                self.stdout.write(f'  → Layer bounds: {layer_gdf.total_bounds}')

            # =====================================================================
            # Step 7: Save Results (if requested)
            # =====================================================================
            if options['output']:
                if verbosity >= 1:
                    self.stdout.write(self.style.WARNING('[Step 7] Saving Results...'))

                output_file = options['output']
                try:
                    if output_file.endswith('.geojson'):
                        overlay_gdf.to_file(output_file, driver='GeoJSON')
                        self.stdout.write(self.style.SUCCESS(f'✓ Saved to {output_file} (GeoJSON)'))

                    elif output_file.endswith('.csv'):
                        # Drop geometry for CSV
                        overlay_gdf.drop('geometry', axis=1).to_csv(output_file, index=False)
                        self.stdout.write(self.style.SUCCESS(f'✓ Saved to {output_file} (CSV)'))

                    elif output_file.endswith(('.xlsx', '.xls')):
                        overlay_gdf.drop('geometry', axis=1).to_excel(output_file, index=False)
                        self.stdout.write(self.style.SUCCESS(f'✓ Saved to {output_file} (Excel)'))

                    else:
                        raise CommandError(
                            'Unsupported format. Use .geojson, .csv, or .xlsx'
                        )

                except Exception as e:
                    raise CommandError(f'Error saving results: {str(e)}')

            # =====================================================================
            # Summary
            # =====================================================================
            self.stdout.write(self.style.SUCCESS('\n' + '='*80))
            self.stdout.write(self.style.SUCCESS('✓ Test Complete!'))
            self.stdout.write(self.style.SUCCESS('='*80 + '\n'))

        except CommandError as e:
            self.stdout.write(self.style.ERROR(f'\n✗ Error: {str(e)}\n'))
            sys.exit(1)

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'\n✗ Unexpected error: {str(e)}\n'))
            import traceback
            if options.get('verbosity', 1) >= 2:
                traceback.print_exc()
            sys.exit(1)

    def _display_table(self, gdf, limit, show_cols, all_columns=False, select_columns=None):
        """Display results as a formatted table"""
        self.stdout.write('\n📊 Sample Results (table format):\n')

        display_df = gdf.head(limit).copy()
        columns = list(display_df.columns)
        
        # Determine which columns to show
        if select_columns:
            # Use user-specified columns
            selected = [c.strip() for c in select_columns.split(',')]
            display_cols = [c for c in selected if c in columns]
            if not display_cols:
                self.stdout.write(f'✗ Warning: None of the specified columns found in data.')
                self.stdout.write(f'  Available columns: {columns}')
                display_cols = [c for c in columns if c != 'geometry'][:show_cols]
        elif all_columns:
            # Show all columns except geometry
            display_cols = [c for c in columns if c != 'geometry']
        else:
            # Show limited columns
            display_cols = [c for c in columns if c != 'geometry'][:show_cols]

        # Print header
        header = ' | '.join(f'{col[:30]:30}' for col in display_cols)
        self.stdout.write(header)
        self.stdout.write('-' * len(header))

        # Print rows
        for idx, row in display_df[display_cols].iterrows():
            values = []
            for col in display_cols:
                val = str(row[col])[:30] if pd.notna(row[col]) else 'None'
                values.append(f'{val:30}')
            self.stdout.write(' | '.join(values))

        if len(gdf) > limit:
            self.stdout.write(f'\n... and {len(gdf) - limit} more rows')

    def _display_json(self, gdf, limit, all_columns=False, select_columns=None):
        """Display results as JSON"""
        self.stdout.write('\n📊 Sample Results (JSON format):\n')

        display_df = gdf.head(limit).drop('geometry', axis=1, errors='ignore')
        
        # Apply column filters
        if select_columns:
            selected = [c.strip() for c in select_columns.split(',')]
            available = [c for c in selected if c in display_df.columns]
            if available:
                display_df = display_df[available]
        elif not all_columns:
            display_df = display_df.iloc[:, :5]  # Default: first 5 columns

        result = {
            "total_features": len(gdf),
            "sample": display_df.to_dict(orient='records')
        }

        self.stdout.write(json.dumps(result, indent=2, default=str))

    def _display_csv(self, gdf, limit, all_columns=False, select_columns=None):
        """Display results as CSV"""
        self.stdout.write('\n📊 Sample Results (CSV format):\n')

        display_df = gdf.head(limit).drop('geometry', axis=1, errors='ignore')
        
        # Apply column filters
        if select_columns:
            selected = [c.strip() for c in select_columns.split(',')]
            available = [c for c in selected if c in display_df.columns]
            if available:
                display_df = display_df[available]
        elif not all_columns:
            display_df = display_df.iloc[:, :5]  # Default: first 5 columns

        self.stdout.write(display_df.to_csv(index=False))

        if len(gdf) > limit:
            self.stdout.write(f'\n... and {len(gdf) - limit} more rows')


# Import pandas for CSV handling
try:
    import pandas as pd
except ImportError:
    pd = None
