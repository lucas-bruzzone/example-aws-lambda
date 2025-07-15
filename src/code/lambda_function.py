import json
import boto3
import uuid
import os
from datetime import datetime, timezone
from typing import Dict, Any, List
import logging
from decimal import Decimal, InvalidOperation
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

# Configurar logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Inicializar DynamoDB
dynamodb = boto3.resource("dynamodb")
table_name = os.environ.get("PROPERTIES_TABLE", "properties")
table = dynamodb.Table(table_name)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Router principal para todas as operações CRUD de propriedades
    """
    try:
        # Log do evento recebido
        method = event.get("httpMethod", "")
        path = event.get("path", "")
        resource = event.get("resource", "")

        logger.info(f"Evento recebido: {method} {path}")

        # Extrair user ID do token JWT para todas as operações
        user_id = extract_user_id(event)
        if not user_id:
            return create_response(
                401, {"error": "Token inválido ou usuário não identificado"}
            )

        # Router baseado no método HTTP e path
        if method == "POST" and "/properties" in resource:
            return create_property(event, user_id)

        elif method == "GET" and "/properties" in resource and "{id}" not in resource:
            return get_properties(event, user_id)

        elif method == "PUT" and "/properties" in resource and "{id}" in resource:
            return update_property(event, user_id)

        elif method == "DELETE" and "/properties" in resource and "{id}" in resource:
            return delete_property(event, user_id)

        elif method == "OPTIONS":
            # Resposta para CORS preflight
            return create_response(200, {"message": "CORS preflight"})

        else:
            return create_response(
                404, {"error": f"Endpoint não encontrado: {method} {resource}"}
            )

    except Exception as e:
        logger.error(f"Erro interno no router: {str(e)}")
        return create_response(500, {"error": "Erro interno do servidor"})


def create_property(event: Dict[str, Any], user_id: str) -> Dict[str, Any]:
    """
    Cria uma nova propriedade
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
            "coordinates": body["coordinates"],
            "createdAt": current_time,
            "updatedAt": current_time,
        }

        # Salvar no DynamoDB
        table.put_item(Item=property_item)

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
            "name": "name",
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
        "coordinates": property_item.get("coordinates", []),
        "createdAt": property_item.get("createdAt"),
        "updatedAt": property_item.get("updatedAt"),
    }


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
