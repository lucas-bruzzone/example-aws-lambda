import json
import boto3
import uuid
import os
import base64
from datetime import datetime, timezone
from typing import Dict, Any, List
import logging
from decimal import Decimal, InvalidOperation
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.units import inch



# Configurar logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
eventbridge = boto3.client("events")

# Adicionar variáveis de ambiente
ANALYSIS_TABLE = os.environ.get("PROPERTY_ANALYSIS_TABLE", "")
EVENTBRIDGE_BUS = os.environ.get("EVENTBRIDGE_BUS_NAME", "")
table_name = os.environ.get("PROPERTIES_TABLE", "properties")
table = dynamodb.Table(table_name)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Router principal para todas as operações CRUD de propriedades
    """
    try:
        method = event.get("httpMethod", "")
        path = event.get("path", "")
        resource = event.get("resource", "")

        logger.info(f"Evento recebido: {method} {path}")

        user_id = extract_user_id(event)
        if not user_id:
            return create_response(
                401, {"error": "Token inválido ou usuário não identificado"}
            )

        # Router com nova rota
        if method == "GET" and "/properties/{id}/analysis" in resource:
            return get_property_analysis(event, user_id)

        elif method == "POST" and "/properties/report" in resource:
            return generate_pdf_report(event, user_id)

        elif method == "POST" and "/properties/import" in resource:
            return import_properties(event, user_id)

        elif method == "POST" and "/properties" in resource and "{id}" not in resource:
            return create_property(event, user_id)

        elif method == "GET" and "/properties" in resource and "{id}" not in resource:
            return get_properties(event, user_id)

        elif method == "PUT" and "/properties" in resource and "{id}" in resource:
            return update_property(event, user_id)

        elif method == "DELETE" and "/properties" in resource and "{id}" in resource:
            return delete_property(event, user_id)

        elif method == "OPTIONS":
            return create_response(200, {"message": "CORS preflight"})

        else:
            return create_response(
                404, {"error": f"Endpoint não encontrado: {method} {resource}"}
            )

    except Exception as e:
        logger.error(f"Erro interno no router: {str(e)}")
        return create_response(500, {"error": "Erro interno do servidor"})


def import_properties(event: Dict[str, Any], user_id: str) -> Dict[str, Any]:
    """
    Importa múltiplas propriedades via lista de dados
    """
    try:
        # Parse do body
        try:
            body = json.loads(event.get("body", "{}"))
        except json.JSONDecodeError:
            return create_response(400, {"error": "JSON inválido"})

        properties_data = body.get("properties", [])
        if not properties_data:
            return create_response(
                400, {"error": "Lista de propriedades é obrigatória"}
            )

        if len(properties_data) > 100:
            return create_response(
                400, {"error": "Máximo 100 propriedades por importação"}
            )

        # Validar e processar propriedades
        successful_imports = []
        failed_imports = []
        current_time = datetime.now(timezone.utc).isoformat()

        # Preparar items para inserção em lote
        items_to_insert = []

        for i, property_data in enumerate(properties_data):
            try:
                # Validar dados da propriedade
                validation_result = validate_property_data(property_data)
                if not validation_result["valid"]:
                    failed_imports.append(
                        {
                            "index": i + 1,
                            "name": property_data.get("name", "Unnamed"),
                            "error": validation_result["message"],
                        }
                    )
                    continue

                # Gerar ID único para a propriedade
                property_id = f"prop_{uuid.uuid4().hex[:12]}"

                # Preparar item para DynamoDB
                property_item = {
                    "userId": user_id,
                    "propertyId": property_id,
                    "name": property_data["name"],
                    "type": property_data.get("type", "fazenda"),
                    "description": property_data.get("description", ""),
                    "area": Decimal(str(property_data["area"])),
                    "perimeter": Decimal(str(property_data["perimeter"])),
                    "coordinates": convert_coordinates_to_decimal(
                        property_data["coordinates"]
                    ),
                    "createdAt": current_time,
                    "updatedAt": current_time,
                }

                items_to_insert.append(property_item)
                successful_imports.append(
                    {"index": i + 1, "name": property_data["name"], "id": property_id}
                )

            except Exception as e:
                failed_imports.append(
                    {
                        "index": i + 1,
                        "name": property_data.get("name", "Unnamed"),
                        "error": str(e),
                    }
                )

        # Inserir propriedades em lotes usando batch_writer
        if items_to_insert:
            try:
                insert_result = batch_insert_properties(items_to_insert)
                if not insert_result["success"]:
                    logger.error(
                        f"Erro na inserção em lote: {insert_result['message']}"
                    )
                    # Se falhou a inserção em lote, tentar inserções individuais
                    individual_result = insert_properties_individually(items_to_insert)
                    successful_count = individual_result["successful"]
                else:
                    successful_count = len(items_to_insert)
            except Exception as e:
                logger.error(f"Erro na inserção de propriedades: {str(e)}")
                return create_response(
                    500, {"error": "Erro ao inserir propriedades no banco"}
                )
        else:
            successful_count = 0

        logger.info(
            f"Importação concluída: {successful_count} sucessos, {len(failed_imports)} falhas"
        )

        return create_response(
            200,
            {
                "message": "Importação processada",
                "imported": successful_count,
                "failed": len(failed_imports),
                "total": len(properties_data),
                "successful_imports": successful_imports,
                "failed_imports": failed_imports[:10],  # Limitar erros retornados
            },
        )

    except Exception as e:
        logger.error(f"Erro na importação: {str(e)}")
        return create_response(500, {"error": "Erro na importação de propriedades"})


def batch_insert_properties(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Insere propriedades em lote usando batch_writer
    """
    try:
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)

        return {"success": True, "message": f"{len(items)} propriedades inseridas"}

    except Exception as e:
        logger.error(f"Erro na inserção em lote: {str(e)}")
        return {"success": False, "message": str(e)}


def insert_properties_individually(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Insere propriedades individualmente como fallback
    """
    successful = 0
    failed = 0

    for item in items:
        try:
            table.put_item(Item=item)
            successful += 1
        except Exception as e:
            logger.error(f"Erro ao inserir propriedade {item.get('name')}: {str(e)}")
            failed += 1

    return {"successful": successful, "failed": failed}


def generate_pdf_report(event: Dict[str, Any], user_id: str) -> Dict[str, Any]:
    """
    Gera relatório PDF das propriedades selecionadas
    """
    try:
        # Parse do body
        try:
            body = json.loads(event.get("body", "{}"))
        except json.JSONDecodeError:
            return create_response(400, {"error": "JSON inválido"})

        property_ids = body.get("propertyIds", [])
        if not property_ids:
            return create_response(
                400, {"error": "Lista de propriedades é obrigatória"}
            )

        # Buscar propriedades do usuário
        properties = []
        for prop_id in property_ids:
            prop = get_existing_property(user_id, prop_id)
            if prop:
                properties.append(format_property_for_response(prop))

        if not properties:
            return create_response(404, {"error": "Nenhuma propriedade encontrada"})

        # Gerar PDF
        pdf_result = create_pdf_report(properties, user_id)

        if pdf_result["success"]:
            return create_response(
                200,
                {
                    "message": "Relatório gerado com sucesso",
                    "pdf": pdf_result["pdf_base64"],
                    "filename": pdf_result["filename"],
                    "properties_count": len(properties),
                },
            )
        else:
            return create_response(500, {"error": pdf_result["message"]})

    except Exception as e:
        logger.error(f"Erro ao gerar relatório: {str(e)}")
        return create_response(500, {"error": "Erro ao gerar relatório PDF"})


def create_pdf_report(properties: List[Dict[str, Any]], user_id: str) -> Dict[str, Any]:
    """
    Cria o PDF usando ReportLab
    """
    try:
        # Buffer para o PDF
        buffer = BytesIO()

        # Configurar documento
        doc = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            rightMargin=72,
            leftMargin=72,
            topMargin=72,
            bottomMargin=18,
        )

        # Estilos
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            "CustomTitle",
            parent=styles["Heading1"],
            fontSize=18,
            spaceAfter=30,
            textColor=colors.HexColor("#7c3aed"),
            alignment=1,  # Centralizado
        )

        subtitle_style = ParagraphStyle(
            "Subtitle",
            parent=styles["Normal"],
            fontSize=12,
            spaceAfter=20,
            textColor=colors.HexColor("#4b5563"),
            alignment=1,
        )

        # Conteúdo do PDF
        story = []

        # Título
        story.append(Paragraph("Relatório de Propriedades Rurais", title_style))

        # Data
        current_date = datetime.now(timezone.utc)
        formatted_date = current_date.strftime("%d de %B de %Y às %H:%M UTC")
        story.append(Paragraph(f"Gerado em: {formatted_date}", subtitle_style))
        story.append(Spacer(1, 20))

        # Resumo executivo
        story.append(Paragraph("<b>RESUMO EXECUTIVO</b>", styles["Heading2"]))
        story.append(Spacer(1, 12))

        total_area = sum(p.get("area", 0) for p in properties)
        total_perimeter = sum(p.get("perimeter", 0) for p in properties)
        avg_area = total_area / len(properties) if properties else 0

        summary_data = [
            f"<b>Total de propriedades:</b> {len(properties)}",
            f"<b>Área total:</b> {total_area:.2f} hectares",
            f"<b>Perímetro total:</b> {total_perimeter/1000:.2f} km",
            f"<b>Área média:</b> {avg_area:.2f} hectares por propriedade",
        ]

        for item in summary_data:
            story.append(Paragraph(f"• {item}", styles["Normal"]))
            story.append(Spacer(1, 6))

        story.append(Spacer(1, 20))

        # Distribuição por tipo
        type_counts = {}
        for prop in properties:
            prop_type = prop.get("type", "outros").capitalize()
            type_counts[prop_type] = type_counts.get(prop_type, 0) + 1

        story.append(Paragraph("<b>DISTRIBUIÇÃO POR TIPO</b>", styles["Heading2"]))
        story.append(Spacer(1, 12))

        for prop_type, count in type_counts.items():
            percentage = (count / len(properties)) * 100
            story.append(
                Paragraph(
                    f"• <b>{prop_type}:</b> {count} propriedade(s) ({percentage:.1f}%)",
                    styles["Normal"],
                )
            )

        story.append(Spacer(1, 20))

        # Tabela de propriedades
        story.append(Paragraph("<b>PROPRIEDADES DETALHADAS</b>", styles["Heading2"]))
        story.append(Spacer(1, 12))

        # Dados da tabela
        table_data = [
            [
                "Nome da Propriedade",
                "Tipo",
                "Área (ha)",
                "Perímetro (m)",
                "Data Criação",
            ]
        ]

        for prop in properties:
            created_date = prop.get("createdAt", "")
            if created_date:
                try:
                    date_obj = datetime.fromisoformat(
                        created_date.replace("Z", "+00:00")
                    )
                    formatted_created = date_obj.strftime("%d/%m/%Y")
                except:
                    formatted_created = "N/A"
            else:
                formatted_created = "N/A"

            # Truncar nome se muito longo
            name = prop.get("name", "")
            if len(name) > 30:
                name = name[:27] + "..."

            table_data.append(
                [
                    name,
                    prop.get("type", "").capitalize(),
                    f"{prop.get('area', 0):.2f}",
                    f"{prop.get('perimeter', 0):.0f}",
                    formatted_created,
                ]
            )

        # Criar tabela
        table = Table(
            table_data, colWidths=[2.5 * inch, 1 * inch, 0.8 * inch, 1 * inch, 1 * inch]
        )
        table.setStyle(
            TableStyle(
                [
                    # Header
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#7c3aed")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 9),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
                    # Body
                    ("BACKGROUND", (0, 1), (-1, -1), colors.white),
                    ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                    ("FONTSIZE", (0, 1), (-1, -1), 8),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    (
                        "ROWBACKGROUNDS",
                        (0, 1),
                        (-1, -1),
                        [colors.white, colors.HexColor("#f9fafb")],
                    ),
                ]
            )
        )

        story.append(table)

        # Estatísticas adicionais
        if len(properties) > 1:
            story.append(Spacer(1, 30))
            story.append(
                Paragraph("<b>ESTATÍSTICAS ADICIONAIS</b>", styles["Heading2"])
            )
            story.append(Spacer(1, 12))

            areas = [p.get("area", 0) for p in properties if p.get("area", 0) > 0]
            if areas:
                largest_area = max(areas)
                smallest_area = min(areas)

                largest_prop = next(
                    p for p in properties if p.get("area") == largest_area
                )
                smallest_prop = next(
                    p for p in properties if p.get("area") == smallest_area
                )

                stats_data = [
                    f"• <b>Maior propriedade:</b> {largest_prop.get('name')} - {largest_area:.2f} ha",
                    f"• <b>Menor propriedade:</b> {smallest_prop.get('name')} - {smallest_area:.2f} ha",
                    f"• <b>Diferença:</b> {largest_area - smallest_area:.2f} ha",
                ]

                for stat in stats_data:
                    story.append(Paragraph(stat, styles["Normal"]))
                    story.append(Spacer(1, 6))

        # Rodapé informativo
        story.append(Spacer(1, 30))
        footer_style = ParagraphStyle(
            "Footer",
            parent=styles["Normal"],
            fontSize=8,
            textColor=colors.HexColor("#6b7280"),
            alignment=1,
        )
        story.append(
            Paragraph(
                "Sistema Rural - Desenvolvido por Lucas Bruzzone, Cientista de Dados<br/>"
                "Relatório gerado automaticamente via AWS Lambda",
                footer_style,
            )
        )

        # Construir PDF
        doc.build(story)

        # Converter para base64
        pdf_bytes = buffer.getvalue()
        buffer.close()

        pdf_base64 = base64.b64encode(pdf_bytes).decode("utf-8")

        # Nome do arquivo
        timestamp = current_date.strftime("%Y%m%d_%H%M%S")
        filename = f"relatorio_propriedades_{timestamp}.pdf"

        return {"success": True, "pdf_base64": pdf_base64, "filename": filename}

    except Exception as e:
        logger.error(f"Erro ao criar PDF: {str(e)}")
        return {"success": False, "message": f"Erro ao criar PDF: {str(e)}"}



def create_property(event: Dict[str, Any], user_id: str) -> Dict[str, Any]:
    """
    Cria uma nova propriedade e publica evento
    """
    try:
        # Parse do body
        try:
            body = json.loads(event.get("body", "{}"))
        except json.JSONDecodeError:
            return create_response(
                400, {"error": "JSON inválido no body da requisição"}
            )

        # Validar dados da propriedade
        validation_result = validate_property_data(body)
        if not validation_result["valid"]:
            return create_response(400, {"error": validation_result["message"]})

        # Gerar ID único para a propriedade
        property_id = f"prop_{uuid.uuid4().hex[:12]}"

        # Preparar item para DynamoDB
        current_time = datetime.now(timezone.utc).isoformat()

        property_item = {
            "userId": user_id,
            "propertyId": property_id,
            "name": body["name"],
            "type": body.get("type", "fazenda"),
            "description": body.get("description", ""),
            "area": Decimal(str(body["area"])),
            "perimeter": Decimal(str(body["perimeter"])),
            "coordinates": convert_coordinates_to_decimal(body["coordinates"]),
            "analysisStatus": "pending",  # Novo campo
            "createdAt": current_time,
            "updatedAt": current_time,
        }

        # Salvar no DynamoDB
        table.put_item(Item=property_item)

        # Publicar evento para análise geoespacial
        try:
            publish_property_created_event(property_id, body["coordinates"], user_id)
        except Exception as e:
            logger.warning(f"Erro ao publicar evento: {str(e)}")

        logger.info(
            f"Propriedade criada com sucesso: {property_id} para usuário {user_id[:8]}..."
        )

        # Formatar resposta
        formatted_property = format_property_for_response(property_item)

        return create_response(
            201,
            {
                "message": "Propriedade criada com sucesso",
                "property": formatted_property,
            },
        )

    except Exception as e:
        logger.error(f"Erro ao criar propriedade: {str(e)}")
        return create_response(500, {"error": "Erro interno do servidor"})


def get_properties(event: Dict[str, Any], user_id: str) -> Dict[str, Any]:
    """
    Lista propriedades do usuário
    """
    try:
        # Extrair parâmetros de consulta
        query_params = event.get("queryStringParameters") or {}

        # Parâmetros de filtro e paginação
        property_type = query_params.get("type")
        limit = int(query_params.get("limit", 50))
        last_key = query_params.get("lastKey")

        # Validar limite
        if limit < 1 or limit > 100:
            limit = 50

        # Buscar propriedades do usuário
        result = get_user_properties(user_id, property_type, limit, last_key)

        if result["success"]:
            logger.info(
                f"Retornando {len(result['properties'])} propriedades para usuário {user_id[:8]}..."
            )

            response_data = {
                "properties": result["properties"],
                "count": len(result["properties"]),
                "lastKey": result.get("lastKey"),
            }

            # Adicionar estatísticas
            if result["properties"]:
                stats = calculate_stats(result["properties"])
                response_data["statistics"] = stats

            return create_response(200, response_data)
        else:
            return create_response(500, {"error": result["message"]})

    except Exception as e:
        logger.error(f"Erro ao buscar propriedades: {str(e)}")
        return create_response(500, {"error": "Erro interno do servidor"})


def update_property(event: Dict[str, Any], user_id: str) -> Dict[str, Any]:
    """
    Atualiza uma propriedade existente
    """
    try:
        # Extrair property ID da URL
        path_parameters = event.get("pathParameters") or {}
        property_id = path_parameters.get("id")

        if not property_id:
            return create_response(400, {"error": "ID da propriedade é obrigatório"})

        # Parse do body
        try:
            body = json.loads(event.get("body", "{}"))
        except json.JSONDecodeError:
            return create_response(
                400, {"error": "JSON inválido no body da requisição"}
            )

        if not body:
            return create_response(
                400, {"error": "Body da requisição não pode estar vazio"}
            )

        # Verificar se a propriedade existe e pertence ao usuário
        existing_property = get_existing_property(user_id, property_id)
        if not existing_property:
            return create_response(404, {"error": "Propriedade não encontrada"})

        # Validar dados da atualização
        validation_result = validate_update_data(body)
        if not validation_result["valid"]:
            return create_response(400, {"error": validation_result["message"]})

        # Atualizar propriedade
        updated_property = update_property_data(
            user_id, property_id, body, existing_property
        )

        if updated_property["success"]:
            logger.info(
                f"Propriedade {property_id} atualizada com sucesso para usuário {user_id[:8]}..."
            )
            return create_response(
                200,
                {
                    "message": "Propriedade atualizada com sucesso",
                    "property": updated_property["property"],
                },
            )
        else:
            return create_response(500, {"error": updated_property["message"]})

    except Exception as e:
        logger.error(f"Erro ao atualizar propriedade: {str(e)}")
        return create_response(500, {"error": "Erro interno do servidor"})


def delete_property(event: Dict[str, Any], user_id: str) -> Dict[str, Any]:
    """
    Deleta uma propriedade
    """
    try:
        # Extrair property ID da URL
        path_parameters = event.get("pathParameters") or {}
        property_id = path_parameters.get("id")

        if not property_id:
            return create_response(400, {"error": "ID da propriedade é obrigatório"})

        # Verificar se a propriedade existe e pertence ao usuário
        existing_property = get_existing_property(user_id, property_id)
        if not existing_property:
            return create_response(404, {"error": "Propriedade não encontrada"})

        # Deletar propriedade
        deletion_result = delete_property_data(user_id, property_id)

        if deletion_result["success"]:
            logger.info(
                f"Propriedade {property_id} deletada com sucesso para usuário {user_id[:8]}..."
            )
            return create_response(
                200,
                {
                    "message": "Propriedade deletada com sucesso",
                    "deletedProperty": {
                        "id": property_id,
                        "name": existing_property.get("name"),
                    },
                },
            )
        else:
            return create_response(500, {"error": deletion_result["message"]})

    except Exception as e:
        logger.error(f"Erro ao deletar propriedade: {str(e)}")
        return create_response(500, {"error": "Erro interno do servidor"})


# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================


def convert_coordinates_to_decimal(coordinates):
    """
    Converte coordenadas para Decimal (compatível com DynamoDB)
    """
    if not isinstance(coordinates, list):
        return coordinates

    decimal_coordinates = []
    for coord in coordinates:
        if isinstance(coord, list) and len(coord) == 2:
            decimal_coord = [Decimal(str(coord[0])), Decimal(str(coord[1]))]
            decimal_coordinates.append(decimal_coord)
        else:
            decimal_coordinates.append(coord)

    return decimal_coordinates


def convert_coordinates_to_float(coordinates):
    """
    Converte coordenadas de Decimal para float (para resposta da API)
    """
    if not isinstance(coordinates, list):
        return coordinates

    float_coordinates = []
    for coord in coordinates:
        if isinstance(coord, list) and len(coord) == 2:
            float_coord = [float(coord[0]), float(coord[1])]
            float_coordinates.append(float_coord)
        else:
            float_coordinates.append(coord)

    return float_coordinates


def get_user_properties(
    user_id: str, property_type: str = None, limit: int = 50, last_key: str = None
) -> Dict[str, Any]:
    """
    Busca propriedades do usuário no DynamoDB
    """
    try:
        # Parâmetros base da consulta
        query_params = {
            "KeyConditionExpression": Key("userId").eq(user_id),
            "Limit": limit,
            "ScanIndexForward": False,  # Ordenar por data (mais recentes primeiro)
        }

        # Adicionar paginação se fornecida
        if last_key:
            try:
                decoded_key = json.loads(last_key)
                query_params["ExclusiveStartKey"] = decoded_key
            except json.JSONDecodeError:
                logger.warning(f"LastKey inválido: {last_key}")

        # Executar consulta
        response = table.query(**query_params)

        properties = response.get("Items", [])

        # Filtrar por tipo se especificado
        if property_type:
            properties = [p for p in properties if p.get("type") == property_type]

        # Preparar próxima chave para paginação
        next_key = None
        if "LastEvaluatedKey" in response:
            next_key = json.dumps(response["LastEvaluatedKey"], default=str)

        # Formatar propriedades para resposta
        formatted_properties = []
        for prop in properties:
            formatted_prop = format_property_for_response(prop)
            formatted_properties.append(formatted_prop)

        return {
            "success": True,
            "properties": formatted_properties,
            "lastKey": next_key,
        }

    except Exception as e:
        logger.error(f"Erro ao buscar propriedades: {str(e)}")
        return {"success": False, "message": f"Erro ao buscar propriedades: {str(e)}"}


def get_existing_property(user_id: str, property_id: str) -> Dict[str, Any]:
    """
    Busca propriedade existente no DynamoDB
    """
    try:
        response = table.get_item(Key={"userId": user_id, "propertyId": property_id})

        return response.get("Item")

    except ClientError as e:
        logger.error(f"Erro ao buscar propriedade existente: {str(e)}")
        return None


def update_property_data(
    user_id: str,
    property_id: str,
    update_data: Dict[str, Any],
    existing_property: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Atualiza propriedade no DynamoDB
    """
    try:
        # Preparar dados para atualização
        current_time = datetime.now(timezone.utc).isoformat()

        # Construir expressão de atualização dinamicamente
        update_expression_parts = []
        expression_attribute_values = {}
        expression_attribute_names = {}

        # Campos que podem ser atualizados
        updatable_fields = {
            "name": "#name",  # 'name' é palavra reservada no DynamoDB
            "type": "#type",  # 'type' é palavra reservada no DynamoDB
            "description": "description",
            "area": "area",
            "perimeter": "perimeter",
            "coordinates": "coordinates",
        }

        for field, attr_name in updatable_fields.items():
            if field in update_data:
                update_expression_parts.append(f"{attr_name} = :{field}")

                # Converter números para Decimal
                if field in ["area", "perimeter"]:
                    expression_attribute_values[f":{field}"] = Decimal(
                        str(update_data[field])
                    )
                elif field == "coordinates":
                    expression_attribute_values[f":{field}"] = (
                        convert_coordinates_to_decimal(update_data[field])
                    )
                else:
                    expression_attribute_values[f":{field}"] = update_data[field]

                # Adicionar nome do atributo se necessário
                if attr_name.startswith("#"):
                    expression_attribute_names[attr_name] = field

        # Sempre atualizar o timestamp
        update_expression_parts.append("updatedAt = :updatedAt")
        expression_attribute_values[":updatedAt"] = current_time

        # Construir expressão final
        update_expression = "SET " + ", ".join(update_expression_parts)

        # Executar atualização
        update_params = {
            "Key": {"userId": user_id, "propertyId": property_id},
            "UpdateExpression": update_expression,
            "ExpressionAttributeValues": expression_attribute_values,
            "ReturnValues": "ALL_NEW",
        }

        if expression_attribute_names:
            update_params["ExpressionAttributeNames"] = expression_attribute_names

        response = table.update_item(**update_params)

        # Formatar propriedade atualizada
        updated_item = response["Attributes"]
        formatted_property = format_property_for_response(updated_item)

        return {"success": True, "property": formatted_property}

    except ClientError as e:
        logger.error(f"Erro ao atualizar propriedade: {str(e)}")
        return {"success": False, "message": f"Erro ao atualizar propriedade: {str(e)}"}


def delete_property_data(user_id: str, property_id: str) -> Dict[str, Any]:
    """
    Deleta propriedade do DynamoDB
    """
    try:
        # Usar conditional delete para garantir que o item existe
        table.delete_item(
            Key={"userId": user_id, "propertyId": property_id},
            ConditionExpression="attribute_exists(userId) AND attribute_exists(propertyId)",
        )

        return {"success": True, "message": "Propriedade deletada com sucesso"}

    except ClientError as e:
        error_code = e.response["Error"]["Code"]

        if error_code == "ConditionalCheckFailedException":
            # Item não existe ou não pertence ao usuário
            logger.warning(
                f"Tentativa de deletar propriedade inexistente: {property_id}"
            )
            return {
                "success": False,
                "message": "Propriedade não encontrada ou não pertence ao usuário",
            }
        else:
            logger.error(f"Erro ao deletar propriedade: {str(e)}")
            return {
                "success": False,
                "message": f"Erro ao deletar propriedade: {str(e)}",
            }

    except Exception as e:
        logger.error(f"Erro inesperado ao deletar propriedade: {str(e)}")
        return {"success": False, "message": f"Erro inesperado: {str(e)}"}


def calculate_stats(properties: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calcula estatísticas das propriedades
    """
    try:
        # Usar Decimal para cálculos
        total_area = Decimal("0")
        total_perimeter = Decimal("0")

        areas = []
        for prop in properties:
            area = Decimal(str(prop.get("area", 0)))
            perimeter = Decimal(str(prop.get("perimeter", 0)))

            total_area += area
            total_perimeter += perimeter

            if area > 0:
                areas.append(area)

        # Contagem por tipo
        type_counts = {}
        for prop in properties:
            prop_type = prop.get("type", "outros")
            type_counts[prop_type] = type_counts.get(prop_type, 0) + 1

        # Propriedade maior e menor
        largest_area = max(areas) if areas else Decimal("0")
        smallest_area = min(areas) if areas else Decimal("0")

        # Média
        average_area = total_area / len(properties) if properties else Decimal("0")

        return {
            "totalProperties": len(properties),
            "totalArea": float(total_area),
            "totalPerimeter": float(total_perimeter),
            "averageArea": float(average_area),
            "largestProperty": float(largest_area),
            "smallestProperty": float(smallest_area),
            "typeDistribution": type_counts,
        }

    except Exception as e:
        logger.error(f"Erro ao calcular estatísticas: {str(e)}")
        return {}


def format_property_for_response(property_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Formata propriedade para resposta da API
    """
    return {
        "id": property_item.get("propertyId"),
        "name": property_item.get("name"),
        "type": property_item.get("type"),
        "description": property_item.get("description", ""),
        "area": float(property_item.get("area", 0)),
        "perimeter": float(property_item.get("perimeter", 0)),
        "coordinates": convert_coordinates_to_float(
            property_item.get("coordinates", [])
        ),
        "analysisStatus": property_item.get("analysisStatus", "pending"),  # Novo campo
        "createdAt": property_item.get("createdAt"),
        "updatedAt": property_item.get("updatedAt"),
    }


# Nova função para publicar evento
def publish_property_created_event(property_id: str, coordinates: list, user_id: str):
    """
    Publica evento de propriedade criada para EventBridge
    """
    if not EVENTBRIDGE_BUS:
        logger.warning("EventBridge bus não configurado")
        return

    try:
        eventbridge.put_events(
            Entries=[
                {
                    "Source": "property.service",
                    "DetailType": "Property Created",
                    "Detail": json.dumps(
                        {
                            "propertyId": property_id,
                            "coordinates": coordinates,
                            "userId": user_id,
                            "status": "created",
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }
                    ),
                    "EventBusName": EVENTBRIDGE_BUS,
                }
            ]
        )
        logger.info(f"Evento publicado para propriedade: {property_id}")
    except Exception as e:
        logger.error(f"Erro ao publicar evento: {str(e)}")
        raise


# Nova função para buscar análise
def get_property_analysis(event: Dict[str, Any], user_id: str) -> Dict[str, Any]:
    """
    Retorna análise geoespacial de uma propriedade
    """
    try:
        # Extrair property ID da URL
        path_parameters = event.get("pathParameters") or {}
        property_id = path_parameters.get("id")

        if not property_id:
            return create_response(400, {"error": "ID da propriedade é obrigatório"})

        # Verificar se a propriedade existe e pertence ao usuário
        existing_property = get_existing_property(user_id, property_id)
        if not existing_property:
            return create_response(404, {"error": "Propriedade não encontrada"})

        # Buscar análise na tabela de análises
        analysis_result = get_analysis_data(property_id)

        return create_response(
            200, {"propertyId": property_id, "analysis": analysis_result}
        )

    except Exception as e:
        logger.error(f"Erro ao buscar análise: {str(e)}")
        return create_response(500, {"error": "Erro interno do servidor"})


# Função auxiliar para buscar análise
def get_analysis_data(property_id: str) -> Dict[str, Any]:
    """
    Busca dados de análise no DynamoDB
    """
    if not ANALYSIS_TABLE:
        return {
            "status": "not_configured",
            "message": "Sistema de análise não configurado",
        }

    try:
        analysis_table = dynamodb.Table(ANALYSIS_TABLE)
        response = analysis_table.get_item(Key={"propertyId": property_id})

        if "Item" in response:
            item = response["Item"]

            # Converter Decimal para float
            analysis_data = json.loads(json.dumps(item, default=str))
            return analysis_data
        else:
            return {"status": "pending", "message": "Análise em processamento"}

    except Exception as e:
        logger.error(f"Erro ao buscar análise: {str(e)}")
        return {"status": "error", "message": "Erro ao buscar análise"}


# ============================================================================
# VALIDAÇÕES
# ============================================================================


def validate_property_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Valida os dados da propriedade para criação
    """
    # Campos obrigatórios
    required_fields = ["name", "area", "perimeter", "coordinates"]

    for field in required_fields:
        if field not in data:
            return {"valid": False, "message": f"Campo obrigatório: {field}"}

    # Validar nome
    name = data.get("name", "").strip()
    if not name or len(name) < 2:
        return {"valid": False, "message": "Nome deve ter pelo menos 2 caracteres"}

    if len(name) > 100:
        return {"valid": False, "message": "Nome deve ter no máximo 100 caracteres"}

    # Validar área
    try:
        area = Decimal(str(data["area"]))
        if area <= 0:
            return {"valid": False, "message": "Área deve ser maior que zero"}
        if area > 1000000:  # 1 milhão de hectares (limite razoável)
            return {
                "valid": False,
                "message": "Área muito grande (máximo 1.000.000 hectares)",
            }
    except (ValueError, TypeError, InvalidOperation):
        return {"valid": False, "message": "Área deve ser um número válido"}

    # Validar perímetro
    try:
        perimeter = Decimal(str(data["perimeter"]))
        if perimeter <= 0:
            return {"valid": False, "message": "Perímetro deve ser maior que zero"}
    except (ValueError, TypeError, InvalidOperation):
        return {"valid": False, "message": "Perímetro deve ser um número válido"}

    # Validar coordenadas (GeoJSON)
    coordinates = data.get("coordinates")
    if not validate_coordinates(coordinates):
        return {"valid": False, "message": "Coordenadas em formato inválido"}

    # Validar tipo de propriedade
    valid_types = ["fazenda", "sitio", "chacara", "terreno", "outros"]
    property_type = data.get("type", "fazenda")
    if property_type not in valid_types:
        return {
            "valid": False,
            "message": f'Tipo inválido. Use: {", ".join(valid_types)}',
        }

    # Validar descrição
    description = data.get("description", "")
    if len(description) > 500:
        return {
            "valid": False,
            "message": "Descrição deve ter no máximo 500 caracteres",
        }

    return {"valid": True, "message": "Dados válidos"}


def validate_update_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Valida os dados de atualização da propriedade
    """
    # Pelo menos um campo deve ser fornecido
    updatable_fields = [
        "name",
        "type",
        "description",
        "area",
        "perimeter",
        "coordinates",
    ]

    if not any(field in data for field in updatable_fields):
        return {
            "valid": False,
            "message": "Pelo menos um campo deve ser fornecido para atualização",
        }

    # Validar nome se fornecido
    if "name" in data:
        name = data["name"].strip()
        if not name or len(name) < 2:
            return {"valid": False, "message": "Nome deve ter pelo menos 2 caracteres"}

        if len(name) > 100:
            return {"valid": False, "message": "Nome deve ter no máximo 100 caracteres"}

    # Validar área se fornecida
    if "area" in data:
        try:
            area = Decimal(str(data["area"]))
            if area <= 0:
                return {"valid": False, "message": "Área deve ser maior que zero"}
            if area > 1000000:
                return {
                    "valid": False,
                    "message": "Área muito grande (máximo 1.000.000 hectares)",
                }
        except (ValueError, TypeError, InvalidOperation):
            return {"valid": False, "message": "Área deve ser um número válido"}

    # Validar perímetro se fornecido
    if "perimeter" in data:
        try:
            perimeter = Decimal(str(data["perimeter"]))
            if perimeter <= 0:
                return {"valid": False, "message": "Perímetro deve ser maior que zero"}
        except (ValueError, TypeError, InvalidOperation):
            return {"valid": False, "message": "Perímetro deve ser um número válido"}

    # Validar coordenadas se fornecidas
    if "coordinates" in data:
        if not validate_coordinates(data["coordinates"]):
            return {"valid": False, "message": "Coordenadas em formato inválido"}

    # Validar tipo se fornecido
    if "type" in data:
        valid_types = ["fazenda", "sitio", "chacara", "terreno", "outros"]
        if data["type"] not in valid_types:
            return {
                "valid": False,
                "message": f'Tipo inválido. Use: {", ".join(valid_types)}',
            }

    # Validar descrição se fornecida
    if "description" in data:
        if len(data["description"]) > 500:
            return {
                "valid": False,
                "message": "Descrição deve ter no máximo 500 caracteres",
            }

    return {"valid": True, "message": "Dados válidos"}


def validate_coordinates(coordinates) -> bool:
    """
    Valida se as coordenadas estão em formato GeoJSON válido
    """
    try:
        # Deve ser uma lista
        if not isinstance(coordinates, list):
            return False

        # Deve ter pelo menos 3 pontos (triângulo mínimo)
        if len(coordinates) < 4:  # 3 pontos + fechar o polígono
            return False

        # Cada coordenada deve ser [longitude, latitude]
        for coord in coordinates:
            if not isinstance(coord, list) or len(coord) != 2:
                return False

            try:
                lon, lat = float(coord[0]), float(coord[1])

                # Validar limites geográficos (mundial)
                if not (-180 <= lon <= 180):
                    return False
                if not (-90 <= lat <= 90):
                    return False

            except (ValueError, TypeError):
                return False

        # Verificar se o polígono está fechado
        if coordinates[0] != coordinates[-1]:
            return False

        return True

    except Exception:
        return False


# ============================================================================
# UTILIDADES
# ============================================================================


def extract_user_id(event: Dict[str, Any]) -> str:
    """
    Extrai o user ID do contexto do Cognito Authorizer
    """
    try:
        # Via Cognito Authorizer
        authorizer_context = event.get("requestContext", {}).get("authorizer", {})

        # Tentar diferentes formas de obter o user ID
        user_id = (
            authorizer_context.get("claims", {}).get("sub")
            or authorizer_context.get("sub")
            or event.get("requestContext", {})
            .get("identity", {})
            .get("cognitoIdentityId")
        )

        if user_id:
            logger.info(f"User ID extraído: {user_id[:8]}...")
            return user_id

        logger.warning("User ID não encontrado no contexto")
        return None

    except Exception as e:
        logger.error(f"Erro ao extrair user ID: {str(e)}")
        return None


def create_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Cria resposta HTTP padronizada
    """
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
        },
        "body": json.dumps(body, ensure_ascii=False, default=str),
    }
