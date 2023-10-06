import uuid  # Importe o módulo uuid

from cassandra.cluster import Cluster
from cassandra.query import dict_factory

class ListaDeTarefas:
    def __init__(self):
        self.cluster = Cluster(['localhost'])  # Substitua pelo endereço do seu cluster Cassandra
        self.session = self.cluster.connect('cassandrabanco')  # Substitua 'cassandrabanco' pelo nome do seu keyspace
        self.session.row_factory = dict_factory
        self.create_table()

    def create_table(self):
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS tarefa (
                id UUID PRIMARY KEY,
                descricao TEXT
            )
            """
        )

    def adicionar_tarefa(self, descricao):
        id_tarefa = uuid.uuid4()
        self.session.execute(
            """
            INSERT INTO tarefa (id, descricao) VALUES (%s, %s)
            """,
            (id_tarefa, descricao)
        )
        return str(id_tarefa)

    def listar_tarefas(self):
        rows = self.session.execute("SELECT * FROM tarefa")
        return [{'id': str(row['id']), 'descricao': row['descricao']} for row in rows]

    def remover_tarefa(self, id_tarefa, descricao):
        try:
            uuid_obj = uuid.UUID(id_tarefa)
        except ValueError:
            print("ID da tarefa não é um UUID válido.")
            return

        self.session.execute("DELETE FROM tarefa WHERE id = %s AND descricao = %s", (uuid_obj, descricao))

if __name__ == "__main__":
    lista_tarefas = ListaDeTarefas()

    while True:
        print("\n1. Adicionar Tarefa")
        print("2. Listar Tarefas")
        print("3. Remover Tarefa")
        print("4. Sair")
        
        escolha = input("Escolha uma opção: ")

        if escolha == "1":
            descricao = input("Digite a descrição da tarefa: ")
            id_tarefa = lista_tarefas.adicionar_tarefa(descricao)
            print(f"Tarefa adicionada com ID {id_tarefa}")
        elif escolha == "2":
            tarefas = lista_tarefas.listar_tarefas()
            if not tarefas:
                print("Nenhuma tarefa encontrada.")
            else:
                print("Tarefas:")
                for tarefa in tarefas:
                    print(f"ID: {tarefa['id']}, Descrição: {tarefa['descricao']}")
        elif escolha == "3":
            id_tarefa = input("Digite o ID da tarefa que deseja remover: ")
            descricao = input("Digite a descrição da tarefa que deseja remover: ")
            lista_tarefas.remover_tarefa(id_tarefa, descricao)
            print(f"Tarefa com ID {id_tarefa} removida com sucesso.")
        elif escolha == "4":
            break
        else:
            print("Opção inválida. Escolha novamente.")
